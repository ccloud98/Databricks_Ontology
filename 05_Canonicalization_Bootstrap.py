# Databricks notebook source
# MAGIC %md
# MAGIC ## 05_Canonicalization_Bootstrap *(v2 신규)*
# MAGIC `entity_canonicalization`을 사람이 수기로 채우는 대신, `ai_query`로 자동 제안합니다.
# MAGIC 04에서 청크별로 독립 추출된 엔티티들 중 같은 `class_type` 안에 이름이 여러 개인 경우를 모아,
# MAGIC LLM에게 "동일 실체의 다른 표기"인지 판단시킵니다.
# MAGIC
# MAGIC - **확신도 high**: `entity_canonicalization`에 자동 반영 — 06_Resolve_Entities가 바로 사용합니다.
# MAGIC - **확신도 low**: 자동 반영하지 않고 `canonicalization_review_needed`에 후보로만 남깁니다. 이 단계는
# MAGIC   사람 확인을 없애는 게 아니라, 검토 범위를 "전체 표기 편차"에서 "애매한 것만"으로 좁혀주는 역할입니다.
# MAGIC
# MAGIC 이 노트북 뒷부분에는 **타입 충돌 탐지**도 추가돼 있습니다 — `ai_query` 없이 순수 SQL로, 같은 이름이
# MAGIC 서로 다른 `class_type`으로 추출된 경우(예: "현담"이 어떤 청크에서는 `Competitor`로, 다른 청크에서는
# MAGIC `PortfolioCompany`로 뽑힌 경우)를 기계적으로 찾아 `type_conflict_review_needed`에 남깁니다. 이건 실측으로
# MAGIC 확인된 타입 오염(PortfolioCompany/Sponsor에 무관한 회사가 섞이는 문제)의 증상을 03의 온톨로지 품질과
# MAGIC 무관하게 항상 잡아내기 위한 안전장치입니다.
# MAGIC
# MAGIC ⚠️ **자동으로 고치지는 않습니다**(어느 타입이 "맞는" 타입인지는 판단 근거가 부족해 자동화 위험이 큽니다) —
# MAGIC 탐지해서 표로 보여주는 것까지가 이 단계의 역할입니다. 반영은 `relationship_review_overrides`나 수동 수정으로
# MAGIC 하세요.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "카탈로그명")
dbutils.widgets.text("schema_name", "", "스키마명 (프로젝트 전용 격리)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

assert catalog_name and schema_name, "catalog_name / schema_name 위젯을 모두 채워주세요."

# COMMAND ----------

import json
from pyspark.sql import Row

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.entity_canonicalization (
    class_type STRING, raw_name STRING, canonical_name STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.canonicalization_review_needed (
    class_type STRING,
    suggested_canonical_name STRING,
    raw_names ARRAY<STRING>,
    reasoning STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.type_conflict_review_needed (
    name STRING,
    class_type STRING,
    occurrences BIGINT
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### class_type별 표기 편차 수집
# MAGIC v1의 `05_Resolve_Entities` "① 표기 편차 확인" 셀과 동일한 집계입니다 — 사람이 보던 걸 이제 LLM이 먼저 봅니다.

# COMMAND ----------

variants_rows = spark.sql(f"""
    WITH raw_ents AS (
        SELECT explode(parsed.entities) AS e
        FROM {catalog_name}.{schema_name}.parsed_extractions
    )
    SELECT e.class_type, collect_set(e.name) AS distinct_names, count(DISTINCT e.name) AS n
    FROM raw_ents
    GROUP BY e.class_type
    HAVING n > 1
    ORDER BY n DESC
""").collect()

print(f"표기 편차 검토 대상 class_type: {len(variants_rows)}개")

# COMMAND ----------

# MAGIC %md
# MAGIC ### class_type별 자동 판단 (ai_query)
# MAGIC 한 번에 모든 타입을 던지지 않고 **class_type별로 개별 호출**합니다 — 다른 타입 이름들이 섞여 판단에 잡음을
# MAGIC 주는 걸 막기 위함입니다. (이 판단은 "같은 타입 안에서" 동일 실체 여부만 봅니다 — 타입 배정 자체가 잘못된
# MAGIC 문제는 이 단계가 해결하지 못한다는 점을 다시 한번 유의하세요.)

# COMMAND ----------

canon_schema = {
    "type": "json_schema",
    "json_schema": {
        "name": "canonicalization_proposal",
        "schema": {
            "type": "object",
            "properties": {
                "groups": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "canonical_name": {"type": "string"},
                            "raw_names": {"type": "array", "items": {"type": "string"}},
                            "confidence": {"type": "string", "enum": ["high", "low"]},
                            "reasoning": {"type": "string"}
                        },
                        "required": ["canonical_name", "raw_names", "confidence", "reasoning"]
                    }
                }
            },
            "required": ["groups"]
        },
        "strict": True
    }
}

CANON_PROMPT_TEMPLATE = """당신은 지식그래프 구축을 위해 엔티티 이름의 표기 편차를 정리하는 전문가입니다.
아래는 같은 카테고리('{class_type}')로 추출된 서로 다른 이름 목록입니다. 이 중 **동일한 실체를 가리키는
다른 표기**가 있다면 canonical_name(대표 표기) 아래로 묶어 제안하세요.

# 규칙
1. 국문/영문 병기, 약어/전체명, 띄어쓰기·구두점 차이처럼 **표기만 다른 경우** confidence="high"
2. 실제로 동일 실체인지 애매하거나, 상위/하위 개념 관계(예: 특정 모델과 그 모델을 포함하는 상위 프로그램)일 수
   있는 경우 confidence="low" — 명확하지 않으면 절대 high로 표시하지 말 것
3. 이름이 비슷해 보여도 실제로는 다른 실체(계열사, 자회사, 유사 상표의 다른 회사 등)라면 **묶지 말 것**
4. 동의어가 없는(단독으로 남는) 이름은 결과에 포함하지 말 것 — 그룹은 이름이 2개 이상일 때만 제안
5. 대표 표기(canonical_name)는 목록 안에 실제로 있는 이름 중 가장 완전하고 공식적인 형태를 고를 것

# 대상 이름 목록 ({class_type})
{name_list}
"""

all_proposals = []
for row in variants_rows:
    name_list_str = "\n".join(f"- {n}" for n in row.distinct_names)
    prompt = CANON_PROMPT_TEMPLATE.format(class_type=row.class_type, name_list=name_list_str)
    prompt_escaped = prompt.replace("'", "''")
    schema_escaped = json.dumps(canon_schema, ensure_ascii=False).replace("'", "''")

    result_df = spark.sql(f"""
        SELECT ai_query('databricks-claude-sonnet-4-6', '{prompt_escaped}',
                         responseFormat => '{schema_escaped}') AS proposal
    """)
    proposal = json.loads(result_df.collect()[0].proposal)
    for g in proposal["groups"]:
        all_proposals.append({
            "class_type": row.class_type,
            "canonical_name": g["canonical_name"],
            "raw_names": g["raw_names"],
            "confidence": g["confidence"],
            "reasoning": g["reasoning"],
        })

n_high = sum(1 for p in all_proposals if p["confidence"] == "high")
n_low = sum(1 for p in all_proposals if p["confidence"] == "low")
print(f"제안된 그룹 수: {len(all_proposals)}개 (high: {n_high}, low: {n_low})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### high 확신 그룹 → entity_canonicalization 자동 반영

# COMMAND ----------

high_rows = []
for p in all_proposals:
    if p["confidence"] != "high":
        continue
    for raw_name in p["raw_names"]:
        if raw_name == p["canonical_name"]:
            continue  # 대표 표기 자기 자신은 규칙을 만들 필요 없음 (COALESCE가 raw_name 그대로 사용)
        high_rows.append(Row(class_type=p["class_type"], raw_name=raw_name, canonical_name=p["canonical_name"]))

if high_rows:
    spark.createDataFrame(high_rows).createOrReplaceTempView("_new_canon_rules")
    spark.sql(f"""
        MERGE INTO {catalog_name}.{schema_name}.entity_canonicalization t
        USING _new_canon_rules s
        ON t.class_type = s.class_type AND t.raw_name = s.raw_name
        WHEN NOT MATCHED THEN INSERT (class_type, raw_name, canonical_name)
            VALUES (s.class_type, s.raw_name, s.canonical_name)
    """)

print(f"✅ entity_canonicalization 자동 반영: {len(high_rows)}건")

# COMMAND ----------

# MAGIC %md
# MAGIC ### low 확신 그룹 → canonicalization_review_needed에 기록 (자동 반영 안 함, 사람 검토용)

# COMMAND ----------

low_rows = [
    Row(class_type=p["class_type"], suggested_canonical_name=p["canonical_name"],
        raw_names=p["raw_names"], reasoning=p["reasoning"])
    for p in all_proposals if p["confidence"] == "low"
]

if low_rows:
    spark.createDataFrame(low_rows).write.mode("overwrite") \
        .saveAsTable(f"{catalog_name}.{schema_name}.canonicalization_review_needed")
else:
    spark.createDataFrame([], "class_type STRING, suggested_canonical_name STRING, raw_names ARRAY<STRING>, reasoning STRING") \
        .write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.canonicalization_review_needed")

n_review = spark.table(f"{catalog_name}.{schema_name}.canonicalization_review_needed").count()
print(f"⚠️ 사람 검토 대기 중인 애매한 그룹: {n_review}건")
display(spark.table(f"{catalog_name}.{schema_name}.canonicalization_review_needed"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### (선택) 검토 대기 그룹을 사람이 승인해서 반영하고 싶다면
# MAGIC 위 표를 보고 실제로 동일 실체라고 판단되는 행이 있으면, 아래처럼 `entity_canonicalization`에 직접
# MAGIC INSERT한 뒤 06_Resolve_Entities를 실행하세요. (예시 — 실제 review_needed 결과를 보고 채우세요)
# MAGIC ```sql
# MAGIC -- INSERT INTO {catalog_name}.{schema_name}.entity_canonicalization VALUES
# MAGIC -- ('ClassType', '원본표기', '대표표기')
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 타입 충돌 탐지 *(v2 신규 — 여기부터 ai_query 없이 순수 SQL)*
# MAGIC 위까지는 "같은 타입 안에서 이름이 여러 개"(동의어)를 다뤘습니다. 여기서는 반대 패턴 — **같은 이름이
# MAGIC 서로 다른 타입으로 추출된 경우**를 찾습니다. 실측 사례(코아비스 IM v1): "현담"이 일부 청크에서는
# MAGIC `Competitor`로, 다른 청크에서는 `PortfolioCompany`로 추출되어 그래프상 별개 노드로 쪼개졌습니다.
# MAGIC 이런 이름은 대개 03의 타입 정의가 애매하거나(경계가 겹침), 04가 문맥에 따라 다르게 판단한 경우입니다 —
# MAGIC 03의 온톨로지 품질이 이번 실행에서 좋았는지 나빴는지와 무관하게 항상 이 기계적 검사로 잡아냅니다.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.type_conflict_review_needed AS
WITH name_types AS (
    SELECT e.name, e.class_type, count(*) AS occurrences
    FROM {catalog_name}.{schema_name}.parsed_extractions
    LATERAL VIEW explode(parsed.entities) t AS e
    GROUP BY e.name, e.class_type
),
conflicted_names AS (
    SELECT name
    FROM name_types
    GROUP BY name
    HAVING count(DISTINCT class_type) > 1
)
SELECT nt.name, nt.class_type, nt.occurrences
FROM name_types nt
JOIN conflicted_names cn ON nt.name = cn.name
ORDER BY nt.name, nt.occurrences DESC
""")

n_conflicts = spark.sql(f"""
    SELECT count(DISTINCT name) AS n FROM {catalog_name}.{schema_name}.type_conflict_review_needed
""").collect()[0].n

print(f"⚠️ 타입 충돌 발견: {n_conflicts}개 이름이 2개 이상의 class_type으로 추출됨")
display(spark.table(f"{catalog_name}.{schema_name}.type_conflict_review_needed"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### (선택) 타입 충돌을 사람이 확인해서 반영하고 싶다면
# MAGIC 위 표에서 한 이름당 여러 `class_type`이 보이면, `occurrences`가 더 큰(더 자주 그렇게 뽑힌) 쪽을
# MAGIC "맞는" 타입으로 보는 게 보통 합리적입니다. 소수 타입 쪽으로 잘못 들어간 관계는
# MAGIC `relationship_review_overrides`에 `action='DELETE'`로 등록해 07에서 제외하거나, 03의 해당 타입 설명을
# MAGIC 더 명확히 고쳐 04를 재실행하는 것으로 근본 정리할 수 있습니다. 이 노트북은 판단까지 자동화하지 않고
# MAGIC 여기서 멈춥니다 — 어느 쪽이 "진짜" 타입인지는 문서 맥락 판단이 필요한 경우가 많기 때문입니다.