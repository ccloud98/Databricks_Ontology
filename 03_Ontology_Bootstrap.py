# Databricks notebook source
# MAGIC %md
# MAGIC ## 03_Ontology_Bootstrap
# MAGIC 02번에서 확보한 섹션 제목+스니펫 압축본을 LLM(`ai_query`)에 전달해 이 문서에 맞는 엔티티 타입/predicate를
# MAGIC 제안받습니다. 결과는 Delta 테이블(`ontology_entity_types`/`ontology_predicates`)로 저장되어 이후
# MAGIC 단계가 그대로 참조합니다.
# MAGIC
# MAGIC ⚠️ 이 노트북은 중간에 **사람 확인 체크포인트**가 있습니다 — 제안된 내용을 검토한 뒤 저장 셀을 실행하세요.
# MAGIC (v1과 동일하게 이 체크포인트는 소프트 게이트입니다 — 이번 v2 변경 범위는 동의어 자동화와 %run 제거
# MAGIC 두 가지이며, 이 체크포인트 자체를 하드 게이트로 바꾸는 건 포함하지 않았습니다.)

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
7. 아래는 다른 인수금융 문서에서 실제로 관찰된 실패 사례입니다 — 같은 실수를 반복하지 마세요:
   > 어느 문서에서 "PortfolioCompany"라는 타입을 "스폰서(사모펀드)가 투자한 회사"로만 정의했더니, 문서 안의
   > "Sponsor 트랙레코드 소개" 페이지에 나열된, **이번 거래와 무관한** 과거 투자 회사 19곳(호텔, 시멘트,
   > 항공 케이터링 업체 등)까지 전부 이 타입 하나로 뭉쳐 들어갔고, 정작 **이번 거래의 실제 차주(대상회사)**와
   > 구분이 안 됐습니다. 금융주선사(예: 증권사)도 같은 식으로 Sponsor/PortfolioCompany에 잘못 섞여 들어갔습니다.
   > → 이를 막으려면: "이번 거래의 실제 당사자(예: 차주/대상회사, 실제 투자자, 금융주선사)"를 위한 타입과,
   > "트랙레코드·비교자료용으로만 언급되는, 이번 거래와 무관한 회사"를 위한 타입을 **처음부터 서로 다른
   > class_type으로 분리**해서 제안하고, 각 타입 설명에 그 구분을 명시적으로 적으세요. 이 문서에 실제로
   > 그런 구분이 필요한 대상이 등장한다면(예: 이 거래의 당사자 vs 비교/참조용으로만 언급되는 회사) 반드시
   > 별도 타입으로 나누세요.

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
# MAGIC ### ★ 사람 확인 체크포인트
# MAGIC 아래 제안 내용을 검토하세요. 수정이 필요하면 이 셀 실행 후 `proposal["entity_types"]`/`proposal["predicates"]`를
# MAGIC 직접 편집하는 셀을 추가한 뒤, 그다음 저장 셀을 실행하세요.
# MAGIC
# MAGIC 실측으로 확인된 흔한 실패 패턴: "이 거래의 당사자"와 "비교/참조용으로만 언급되는 다른 회사"가 같은 타입
# MAGIC (예: PortfolioCompany, Sponsor)으로 뭉뚱그려 제안되는 경우가 있었습니다. 타입 설명에 "이 거래에 한정"
# MAGIC 같은 제약이 없다면 이 단계에서 추가하는 걸 권장합니다.

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
