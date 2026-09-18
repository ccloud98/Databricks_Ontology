# Databricks notebook source
# MAGIC %md
# MAGIC ## 03_Ontology_Bootstrap
# MAGIC 02번에서 확보한 섹션 제목+스니펫 압축본을 LLM(`ai_query`)에 전달해 이 문서에 맞는 엔티티 타입/predicate를
# MAGIC 제안받습니다. 결과는 Delta 테이블(`ontology_entity_types`/`ontology_predicates`)로 저장되어 이후
# MAGIC 단계가 그대로 참조합니다.
# MAGIC
# MAGIC ⚠️ 이 노트북은 중간에 **사람 확인 체크포인트**가 있습니다 — 제안된 내용을 검토한 뒤 저장 셀을 실행하세요.
# MAGIC (v1과 동일하게 이 체크포인트는 소프트 게이트입니다.)
# MAGIC
# MAGIC **설계 변경 이력**: 한때 프롬프트에 실제 문서(코아비스 인수금융 IM)에서 관찰된 구체적 실패 사례를
# MAGIC few-shot으로 박아넣었던 적이 있습니다("PortfolioCompany 타입에 무관한 회사 19곳이 섞인 사례" 등).
# MAGIC 효과는 있었지만, 이 파이프라인의 존재 이유(도메인 무관 범용성)와 맞지 않는 방식이라 제거했습니다 —
# MAGIC 도메인이 바뀔 때마다 그 도메인에 맞는 새 예시를 또 하드코딩해야 한다면 재사용 가능한 구조가 아닙니다.
# MAGIC 대신 아래 "타입 혼동 위험 자기 비판" 셀에서, 같은 문제(핵심 주제 대상과 비교·참조용 대상의 혼동)를
# MAGIC **도메인과 무관하게 매번 같은 질문**으로 점검합니다.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "카탈로그명")
dbutils.widgets.text("schema_name", "", "스키마명 (프로젝트 전용 격리)")
dbutils.widgets.text("document_domain_hint", "", "문서 도메인 설명 (예: 인수금융 Information Memorandum(IM))")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
document_domain_hint = dbutils.widgets.get("document_domain_hint")

assert catalog_name and schema_name and document_domain_hint, \
    "catalog_name / schema_name / document_domain_hint 위젯을 모두 채워주세요."

# COMMAND ----------

import json
from pyspark.sql.functions import substring

# COMMAND ----------

# MAGIC %md
# MAGIC ### 압축본 생성 — 전체 섹션 제목 + 각 청크 앞부분 스니펫
# MAGIC "일부만 골라서 놓치는" 샘플링이 아니라, 전체 청크의 제목+스니펫을 압축해서 전부 넣는 방식입니다
# MAGIC (문서가 매우 커지는 경우, 이 부분을 섹션 그룹별 1차 요약 → 재요약하는 계층적 압축으로 확장할 수 있습니다).

# COMMAND ----------

sample_rows = (
    spark.table(f"{catalog_name}.{schema_name}.document_chunks")
    .select("chunk_position", "sections_covered", substring("chunk_text", 1, 200).alias("snippet"))
    .orderBy("chunk_position")
    .collect()
)
sample_text = "\n\n".join(f"[{r.sections_covered}]\n{r.snippet}..." for r in sample_rows)
print(f"압축본 길이: {len(sample_text)}자, 청크 {len(sample_rows)}개 반영")

# COMMAND ----------

ontology_proposal_schema = {
    "type": "json_schema",
    "json_schema": {
        "name": "ontology_proposal",
        "schema": {
            "type": "object",
            "properties": {
                "entity_types": {"type": "array", "items": {
                    "type": "object",
                    "properties": {
                        "class_type": {"type": "string"},
                        "description": {"type": "string"},
                        "example_instances": {"type": "array", "items": {"type": "string"}}
                    }, "required": ["class_type", "description"]
                }},
                "predicates": {"type": "array", "items": {
                    "type": "object",
                    "properties": {
                        "predicate": {"type": "string"},
                        "description": {"type": "string"},
                        "example_source_type": {"type": "string"},
                        "example_target_type": {"type": "string"}
                    }, "required": ["predicate", "description"]
                }}
            }, "required": ["entity_types", "predicates"]
        }, "strict": True
    }
}

bootstrap_prompt = f"""당신은 문서에서 온톨로지 그래프 스키마를 설계하는 전문가입니다.
아래는 '{document_domain_hint}' 문서의 대표 발췌본(섹션 제목 + 각 청크 앞부분 스니펫)입니다.
이 문서 전체에서 반복적으로 등장할 핵심 엔티티 카테고리와, 그들 사이의 관계 유형을 제안하세요.

# 규칙
1. 엔티티 카테고리는 PascalCase 영문 식별자로 (예: Sponsor, FinancialProduct)
2. 관계 유형은 SCREAMING_SNAKE_CASE 영문 식별자로 (예: LENDS_TO, HAS_METRIC)
3. 10~15개 내외 엔티티 타입, 10~20개 내외 predicate를 목표로 함 (과도한 세분화 금지)
4. 개인정보(담당자 실명·연락처 등)에 해당하는 카테고리는 제안하지 말 것
5. 이 발췌본에 실제 등장하는 근거가 있는 것만 제안 — 일반적으로 있을 법한 카테고리를 추측해서 채우지 말 것
6. 각 엔티티 타입의 설명(description)에는 "이 거래/문서에 한정된 대상"인지 아닌지를 명확히 표시할 것
   (예: 비교/참조용으로만 언급되는 유사 회사·과거 사례와, 이 문서의 실제 당사자를 같은 타입으로 섞지 말 것)

--- 문서 발췌본 ---
{sample_text}
"""

schema_json_str = json.dumps(ontology_proposal_schema, ensure_ascii=False).replace("'", "''")
prompt_escaped = bootstrap_prompt.replace("'", "''")

proposal_df = spark.sql(f"""
    SELECT ai_query('databricks-claude-sonnet-4-6', '{prompt_escaped}',
                     responseFormat => '{schema_json_str}') AS proposal
""")
proposal = json.loads(proposal_df.collect()[0].proposal)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 자기 일관성 검증
# MAGIC 실측 중 발견된 문제: LLM이 predicate의 `example_source_type`/`example_target_type`으로 자기가 정의한
# MAGIC `entity_types` 목록에 없는 타입을 참조하는 경우가 있었습니다. 저장 전에 이걸 기계적으로 검증합니다.

# COMMAND ----------

defined_types = {e["class_type"] for e in proposal["entity_types"]}
undefined_type_refs = []
for p in proposal["predicates"]:
    for role, ref_type in [("source", p.get("example_source_type")), ("target", p.get("example_target_type"))]:
        if ref_type and ref_type not in defined_types:
            undefined_type_refs.append((p["predicate"], role, ref_type))

if undefined_type_refs:
    print(f"⚠️ entity_types에 정의되지 않은 타입을 참조하는 predicate {len(undefined_type_refs)}건 발견:")
    for predicate, role, ref_type in undefined_type_refs:
        print(f"  - {predicate} ({role}): '{ref_type}' 타입이 entity_types에 없음")
    print("→ 아래 사람 확인 단계에서 entity_types에 해당 타입을 추가하거나, predicate 설명을 수정하세요.")
else:
    print("✅ 모든 predicate의 example_source_type/example_target_type이 정의된 entity_types 안에 있습니다.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 타입 혼동 위험 자기 비판 (domain-agnostic)
# MAGIC 실측 중 반복적으로 관찰된 실패 패턴: "문서의 핵심 주제(실사·분석 대상)"와 "비교·배경·참조 목적으로만
# MAGIC 언급되는 대상"이 한 타입 안에 섞여 제안되는 경우가 있었습니다(예: 인수금융 문서에서 실제 차주와,
# MAGIC 스폰서의 과거 투자 사례 소개용 회사가 같은 타입으로 뭉뚱그려짐). 이 문제를 특정 도메인의 구체적 사례로
# MAGIC 미리 예방하는 대신 — 그러면 도메인이 바뀔 때마다 새 사례를 또 하드코딩해야 하므로 — **매번 같은
# MAGIC 추상적 질문으로 방금 나온 제안 자체를 LLM이 스스로 재검토**하게 합니다. 도메인과 무관하게 항상 동일한
# MAGIC 프롬프트가 실행됩니다.
# MAGIC
# MAGIC 자동으로 고치지는 않습니다(자기 일관성 검증과 동일한 정책) — 경고만 출력하고, 최종 판단은 아래 사람
# MAGIC 확인 체크포인트에서 하도록 남겨둡니다.

# COMMAND ----------

critique_schema = {
    "type": "json_schema",
    "json_schema": {
        "name": "type_conflation_critique",
        "schema": {
            "type": "object",
            "properties": {
                "risky_types": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "class_type": {"type": "string"},
                            "reasoning": {"type": "string"},
                            "suggested_split": {"type": "string"}
                        },
                        "required": ["class_type", "reasoning"]
                    }
                }
            },
            "required": ["risky_types"]
        },
        "strict": True
    }
}

entity_type_desc_for_critique = "\n".join(
    f"- {e['class_type']}: {e['description']}" for e in proposal["entity_types"]
)

critique_prompt = f"""아래는 방금 제안된 온톨로지 엔티티 타입 목록입니다. 각 타입에 대해, 이 문서의 **핵심 주제
(실사·분석 대상 그 자체)**와 **비교·배경·참조 목적으로만 언급되는 대상**이 한 타입 안에 섞여 들어갈 위험이
있는지 검토하세요.

# 판단 기준
1. 타입 설명이 "이 문서/거래의 실제 당사자·핵심 대상"과 "그 외 비교·참조·배경 설명용으로만 언급되는 대상"을
   명확히 구분하고 있지 않다면 위험이 있는 것으로 판단
2. 타입 설명이 이미 충분히 구체적으로 범위를 한정하고 있다면(예: "이 문서의 실제 당사자에 한정") 위험 없음
3. 애매하면 위험 있음으로 판단할 것 (놓치는 것보다 과하게 잡는 게 안전함)
4. 위험이 있다고 판단한 타입에는, 어떻게 별도 타입으로 분리하면 좋을지 suggested_split에 제안할 것

# 검토 대상 엔티티 타입
{entity_type_desc_for_critique}

위험이 있는 타입만 결과에 포함하세요. 없으면 빈 배열을 반환하세요."""

critique_prompt_escaped = critique_prompt.replace("'", "''")
critique_schema_escaped = json.dumps(critique_schema, ensure_ascii=False).replace("'", "''")

critique_df = spark.sql(f"""
    SELECT ai_query('databricks-claude-sonnet-4-6', '{critique_prompt_escaped}',
                     responseFormat => '{critique_schema_escaped}') AS critique
""")
critique = json.loads(critique_df.collect()[0].critique)

if critique["risky_types"]:
    print(f"⚠️ 타입 혼동 위험이 있다고 판단된 타입 {len(critique['risky_types'])}개:")
    for r in critique["risky_types"]:
        print(f"  - {r['class_type']}: {r['reasoning']}")
        if r.get("suggested_split"):
            print(f"    → 제안: {r['suggested_split']}")
    print("→ 아래 사람 확인 단계에서 필요하면 entity_types를 분리/수정하세요.")
else:
    print("✅ 타입 혼동 위험이 있다고 판단된 타입이 없습니다.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ★ 사람 확인 체크포인트
# MAGIC 아래 제안 내용과, 바로 위 자기 비판 결과를 함께 검토하세요. 수정이 필요하면 이 셀 실행 후
# MAGIC `proposal["entity_types"]`/`proposal["predicates"]`를 직접 편집하는 셀을 추가한 뒤, 그다음 저장 셀을
# MAGIC 실행하세요.

# COMMAND ----------

print(f"제안된 엔티티 타입 {len(proposal['entity_types'])}개:")
for e in proposal["entity_types"]:
    print(f"  - {e['class_type']}: {e['description']} (예: {e.get('example_instances', [])})")

print(f"\n제안된 predicate {len(proposal['predicates'])}개:")
for p in proposal["predicates"]:
    print(f"  - {p['predicate']}: {p['description']} "
          f"({p.get('example_source_type','?')} -> {p.get('example_target_type','?')})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 확정 및 저장 — 검토가 끝났으면 이 셀을 실행하세요

# COMMAND ----------

from pyspark.sql import Row

entity_types_df = spark.createDataFrame([
    Row(class_type=e["class_type"], description=e["description"],
        example_instances=e.get("example_instances", []))
    for e in proposal["entity_types"]
])
entity_types_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.ontology_entity_types")

predicates_df = spark.createDataFrame([
    Row(predicate=p["predicate"], description=p["description"],
        example_source_type=p.get("example_source_type", ""),
        example_target_type=p.get("example_target_type", ""))
    for p in proposal["predicates"]
])
predicates_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.ontology_predicates")

# entity_canonicalization: v2부터는 05_Canonicalization_Bootstrap이 ai_query로 자동 채웁니다.
# 여기서는 스키마만 만들어둡니다(05가 없거나 건너뛰어도 06이 빈 테이블로 안전하게 통과하도록).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.entity_canonicalization (
    class_type STRING, raw_name STRING, canonical_name STRING
) USING DELTA
""")

print(f"✅ 온톨로지 확정 저장 완료: 엔티티 타입 {entity_types_df.count()}개, predicate {predicates_df.count()}개")