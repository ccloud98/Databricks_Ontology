# Databricks notebook source
# MAGIC %md
# MAGIC ## 04_Structured_Extraction
# MAGIC 03번에서 확정된 온톨로지(enum)로 청크별 구조화 추출을 수행합니다.
# MAGIC **방안 A**: 재시도/추적 키를 `section_id`가 아니라 `chunk_id`로 사용합니다.
# MAGIC
# MAGIC v2 변경점: `EXTRACTION_FIXED_RULES`를 (v1처럼 01_Config에서 물려받지 않고) 이 노트북에 직접 정의합니다 —
# MAGIC 이 값을 쓰는 곳이 이 노트북뿐이라 굳이 다른 노트북과 공유할 이유가 없습니다.

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

# MAGIC %md
# MAGIC ### 추출 시 공통으로 적용할 고정 규칙
# MAGIC 온톨로지(타입/predicate 목록)와 달리, 이 규칙들은 도메인에 무관하게 항상 적용되는 원칙이라 여기 고정합니다.

# COMMAND ----------

EXTRACTION_FIXED_RULES = """
1. 주어진 텍스트에 명시적으로 등장하는 사실만 추출한다. 추론이나 일반 상식으로 관계를 만들지 않는다.
2. 동일 엔티티는 항상 표준 명칭으로 통일한다.
3. 금액/비율 등 수치는 attributes 필드에 원문 그대로(단위 포함) 문자열로 기록한다.
4. 담당자 실명, 연락처, 이메일 등 개인정보는 추출 대상이 아니다.
5. 텍스트에 추출할 내용이 없으면 entities와 relationships를 빈 배열로 반환한다.
6. 같은 사실을 중복 추출하지 않는다.
7. 엔티티 타입 설명에 "이 거래/문서에 한정"이라는 제약이 있다면, 비교·참조용으로만 언급되는 다른 회사/거래는
   그 타입으로 추출하지 말고, 더 적합한 타입(예: 비교대상 회사/거래 타입)이 있으면 그쪽으로 분류한다.
""".strip()

# COMMAND ----------

import json
from pyspark.sql.functions import expr, col, from_json

class_types = [r.class_type for r in
               spark.table(f"{catalog_name}.{schema_name}.ontology_entity_types").collect()]
predicates = [r.predicate for r in
              spark.table(f"{catalog_name}.{schema_name}.ontology_predicates").collect()]

entity_type_desc = "\n".join(
    f"- {r.class_type}: {r.description}"
    for r in spark.table(f"{catalog_name}.{schema_name}.ontology_entity_types").collect()
)
predicate_list = ", ".join(predicates)

print(f"엔티티 타입 {len(class_types)}개, predicate {len(predicates)}개 로드 완료")

# COMMAND ----------

response_schema = {
    "type": "json_schema",
    "json_schema": {
        "name": "graph_extraction",
        "schema": {
            "type": "object",
            "properties": {
                "entities": {"type": "array", "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "class_type": {"type": "string", "enum": class_types}
                    }, "required": ["name", "class_type"]
                }},
                "relationships": {"type": "array", "items": {
                    "type": "object",
                    "properties": {
                        "source_name": {"type": "string"},
                        "source_type": {"type": "string", "enum": class_types},
                        "predicate": {"type": "string", "enum": predicates},
                        "target_name": {"type": "string"},
                        "target_type": {"type": "string", "enum": class_types},
                        "attributes": {"type": "string"}
                    }, "required": ["source_name", "source_type", "predicate", "target_name", "target_type"]
                }}
            }, "required": ["entities", "relationships"]
        }, "strict": True
    }
}

extraction_system_prompt = f"""당신은 '{document_domain_hint}' 문서에서 온톨로지 그래프의 노드와 관계를 추출하는 전문가입니다.

# 추출 대상 엔티티 타입 (class_type, 아래 목록 외 타입 사용 금지)
{entity_type_desc}

# 추출 대상 관계 타입 (predicate, 아래 목록 외 타입 사용 금지)
{predicate_list}

# 규칙
{EXTRACTION_FIXED_RULES}

JSON 형식으로만 응답하고, 다른 설명은 추가하지 않는다."""

system_prompt_escaped = extraction_system_prompt.replace("'", "''")
response_format_escaped = json.dumps(response_schema, ensure_ascii=False).replace("'", "''")

PARSED_SCHEMA = ("STRUCT<entities: ARRAY<STRUCT<name:STRING, class_type:STRING>>, "
                  "relationships: ARRAY<STRUCT<source_name:STRING, source_type:STRING, predicate:STRING, "
                  "target_name:STRING, target_type:STRING, attributes:STRING>>>")

EXTRACTION_COLUMNS = ["path", "chunk_id", "chunk_position", "chunk_text",
                       "chunk_to_embed", "sections_covered", "ai_response", "parsed"]

# COMMAND ----------

raw_extraction_df = (
    spark.table(f"{catalog_name}.{schema_name}.document_chunks")
    .withColumn("ai_response", expr(f"""
        ai_query(
            'databricks-claude-sonnet-4-6',
            concat('{system_prompt_escaped}', chr(10), chr(10),
                   '--- 아래는 분석할 텍스트입니다 ---', chr(10), chr(10), chunk_text),
            responseFormat => '{response_format_escaped}',
            failOnError => false
        )
    """))
    .withColumn("parsed", from_json(col("ai_response").getField("result"), PARSED_SCHEMA))
)
raw_extraction_df.select(*EXTRACTION_COLUMNS) \
    .write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.parsed_extractions")

display(
    spark.table(f"{catalog_name}.{schema_name}.parsed_extractions")
    .select("chunk_id", "chunk_position", "ai_response.errorMessage", "parsed.entities", "parsed.relationships")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 실패 청크 재시도 (방안 A — `chunk_id` 기준, `section_id` 아님)
# MAGIC Spark 지연 평가 트랩 방지: `DELETE` 직후 같은 지연 DataFrame으로 바로 `write`하지 않고,
# MAGIC **임시 테이블에 먼저 쓴 뒤 검증하고 원본을 교체**합니다(과거 "섹션 50 유실" 사고의 원인이었던 패턴을 회피).

# COMMAND ----------

retry_chunks = (
    spark.table(f"{catalog_name}.{schema_name}.document_chunks")
    .join(
        spark.table(f"{catalog_name}.{schema_name}.parsed_extractions")
             .filter("parsed.relationships IS NULL")
             .select("chunk_id"),
        on="chunk_id"
    )
)

retry_count = retry_chunks.count()
print(f"재시도 대상 청크: {retry_count}개")

if retry_count > 0:
    retry_result = (
        retry_chunks.withColumn("ai_response", expr(f"""
            ai_query(
                'databricks-claude-sonnet-4-6',
                concat('{system_prompt_escaped}', chr(10), chr(10),
                       '--- 아래는 분석할 텍스트입니다 ---', chr(10), chr(10), chunk_text),
                responseFormat => '{response_format_escaped}',
                modelParameters => named_struct('max_tokens', 8000),
                failOnError => false
            )
        """))
        .withColumn("parsed", from_json(col("ai_response").getField("result"), PARSED_SCHEMA))
    )

    # 1) 임시 테이블에 먼저 저장
    retry_result.select(*EXTRACTION_COLUMNS) \
        .write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}._retry_parsed_tmp")

    # 2) 검증
    still_null = spark.sql(f"""
        SELECT count(*) AS n FROM {catalog_name}.{schema_name}._retry_parsed_tmp
        WHERE parsed.relationships IS NULL
    """).collect()[0].n
    print(f"재시도 후에도 실패: {still_null}건")

    # 3) 원본 교체 (DELETE 후 INSERT — 임시 테이블은 이미 저장이 끝난 상태이므로 지연평가 트랩 없음)
    spark.sql(f"""
        DELETE FROM {catalog_name}.{schema_name}.parsed_extractions
        WHERE chunk_id IN (SELECT chunk_id FROM {catalog_name}.{schema_name}._retry_parsed_tmp)
    """)
    spark.sql(f"""
        INSERT INTO {catalog_name}.{schema_name}.parsed_extractions
        SELECT {", ".join(EXTRACTION_COLUMNS)} FROM {catalog_name}.{schema_name}._retry_parsed_tmp
    """)
    spark.sql(f"DROP TABLE {catalog_name}.{schema_name}._retry_parsed_tmp")

final_null = spark.sql(f"""
    SELECT count(*) AS n FROM {catalog_name}.{schema_name}.parsed_extractions
    WHERE parsed.relationships IS NULL
""").collect()[0].n
print(f"최종 NULL 건수: {final_null}")