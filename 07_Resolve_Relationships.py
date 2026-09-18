# Databricks notebook source
# MAGIC %md
# MAGIC ## 07_Resolve_Relationships
# MAGIC `relationships_resolved` 생성 + 실제 관측된 `(source_type, predicate, target_type)` 조합을 사후 집계해
# MAGIC `ontology_predicate_combos` 테이블로 저장합니다 — 이 조합은 사전 설계가 아니라 데이터에서 발견되는 것이므로
# MAGIC 매 프로젝트마다 이 단계에서 새로 확정됩니다.
# MAGIC
# MAGIC ⚠️ 알려진 구조적 한계(v1과 동일, v2에서도 미해결): 아래 `relationships_resolved` 생성 쿼리는
# MAGIC `entities_resolved`와 INNER JOIN이라, 04에서 관계의 source/target으로만 언급되고 `entities`로는 독립
# MAGIC 추출되지 않은 이름은 조용히 누락됩니다(실측: 코아비스 IM 런에서 원시 관계 387건 → 최종 376건, 11건
# MAGIC 무음 손실). 이번 v2 작업 범위(동의어 자동화, %run 제거)에는 포함되지 않았습니다 — 다음 개선 후보입니다.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "카탈로그명")
dbutils.widgets.text("schema_name", "", "스키마명 (프로젝트 전용 격리)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

assert catalog_name and schema_name, "catalog_name / schema_name 위젯을 모두 채워주세요."

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.relationships_resolved AS
WITH raw_rels AS (
  SELECT pe.chunk_id, r.source_name AS raw_source_name, r.source_type, r.predicate,
         r.target_name AS raw_target_name, r.target_type, r.attributes
  FROM {catalog_name}.{schema_name}.parsed_extractions pe
  LATERAL VIEW explode(pe.parsed.relationships) t AS r
),
canon AS (
  SELECT rr.chunk_id,
    COALESCE(cs.canonical_name, rr.raw_source_name) AS source_name, rr.source_type, rr.predicate,
    COALESCE(ct.canonical_name, rr.raw_target_name) AS target_name, rr.target_type, rr.attributes
  FROM raw_rels rr
  LEFT JOIN {catalog_name}.{schema_name}.entity_canonicalization cs
    ON cs.class_type = rr.source_type AND cs.raw_name = rr.raw_source_name
  LEFT JOIN {catalog_name}.{schema_name}.entity_canonicalization ct
    ON ct.class_type = rr.target_type AND ct.raw_name = rr.raw_target_name
)
SELECT DISTINCT c.source_name, c.source_type, c.predicate, c.target_name, c.target_type, c.attributes,
       es.node_id AS source_node_id, et.node_id AS target_node_id
FROM canon c
JOIN {catalog_name}.{schema_name}.entities_resolved es
  ON es.class_type = c.source_type AND es.name = c.source_name
JOIN {catalog_name}.{schema_name}.entities_resolved et
  ON et.class_type = c.target_type AND et.name = c.target_name
""")

n = spark.table(f"{catalog_name}.{schema_name}.relationships_resolved").count()
print(f"✅ relationships_resolved 생성 완료(리뷰 반영 전): {n}개 엣지")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 리뷰 오버라이드 반영 (도메인 전문가 UI 검토 결과)
# MAGIC 통합 검토 화면(Databricks App)에서 "관계 삭제"로 표시한 항목을 제외합니다.
# MAGIC 이 테이블이 없으면(리뷰 앱을 아직 안 썼다면) 빈 테이블로 만들고 그대로 통과시킵니다.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.relationship_review_overrides (
    source_node_id STRING, predicate STRING, target_node_id STRING,
    source_name STRING, target_name STRING, target_type STRING, source_type STRING,
    action STRING, reviewed_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.relationships_resolved AS
SELECT r.* FROM {catalog_name}.{schema_name}.relationships_resolved r
LEFT ANTI JOIN (
    SELECT source_node_id, predicate, target_node_id
    FROM {catalog_name}.{schema_name}.relationship_review_overrides
    WHERE action = 'DELETE'
) o
ON r.source_node_id = o.source_node_id AND r.predicate = o.predicate AND r.target_node_id = o.target_node_id
""")

n_after = spark.table(f"{catalog_name}.{schema_name}.relationships_resolved").count()
print(f"✅ 리뷰 오버라이드 반영 후: {n_after}개 엣지 (삭제 요청 {n - n_after}건 반영)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 관측된 (source_type, predicate, target_type) 조합 사후 집계
# MAGIC 이후 Genie Space instructions 작성이나, 그래프 DB용 node_*/edge_* 테이블 분할의 근거가 되는 테이블입니다.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.ontology_predicate_combos AS
SELECT source_type, predicate, target_type, count(*) AS n
FROM {catalog_name}.{schema_name}.relationships_resolved
GROUP BY source_type, predicate, target_type
ORDER BY source_type, predicate, target_type
""")

combos = spark.table(f"{catalog_name}.{schema_name}.ontology_predicate_combos")
print(f"✅ 실제 관측된 관계 조합: {combos.count()}가지")
display(combos)