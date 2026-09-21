# Databricks notebook source
# MAGIC %md
# MAGIC ## 07_Resolve_Relationships
# MAGIC `relationships_resolved` 생성 + 실제 관측된 `(source_type, predicate, target_type)` 조합을 사후 집계해
# MAGIC `ontology_predicate_combos` 테이블로 저장합니다 — 이 조합은 사전 설계가 아니라 데이터에서 발견되는 것이므로
# MAGIC 매 프로젝트마다 이 단계에서 새로 확정됩니다.
# MAGIC
# MAGIC ✅ **수정 이력**: 예전엔 아래 `relationships_resolved` 생성 쿼리가 `entities_resolved`와 INNER JOIN이라,
# MAGIC 04에서 관계의 source/target으로만 언급되고 `entities`로는 독립 추출되지 않은 이름이 조용히 누락됐습니다.
# MAGIC `06_Resolve_Entities`가 노드 후보 풀에 관계의 source/target도 UNION으로 포함하도록 수정된 뒤로는 이
# MAGIC 무음 손실이 0건임을 3개 데이터셋에서 직접 대조 쿼리(`NOT EXISTS` 서브쿼리)로 검증했습니다.

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 구조적 고립도(salience) 탐지 *(신규)*
# MAGIC 05의 `type_conflict_review_needed`는 "같은 이름이 서로 다른 타입으로 뽑힌 경우"만 잡습니다 —
# MAGIC "매번 일관되게 틀린 타입으로만 뽑힌 경우"(예: 배경 설명에만 등장하는 회사가 핵심 당사자 타입으로 매번
# MAGIC 일관되게 오분류됨)는 구조적으로 못 잡습니다. 이 섹션은 그 사각지대를 순수 SQL(추가 LLM 호출 없음)로
# MAGIC 보완합니다.
# MAGIC
# MAGIC **아이디어**: 실측으로 확인된 오염 사례들은 전부 그래프에서 유독 고립되어 있었습니다 — 관계 수(degree)가
# MAGIC 1~2건, 연결된 predicate 종류가 1개, 등장 청크도 1~2개뿐이었던 반면, 진짜 핵심 당사자는 수십 건의 관계·
# MAGIC 다양한 predicate·수십 개 청크에 걸쳐 나타났습니다(NLP의 "entity salience" 개념과 유사). 이걸 같은
# MAGIC class_type 내 다른 엔티티들과 상대 비교해서, 유독 고립된 엔티티를 탐지합니다.
# MAGIC
# MAGIC 이 단계는 06/07이 이미 만든 `entities_resolved`/`relationships_resolved`가 있어야 계산 가능해서
# MAGIC 05가 아니라 여기(07 마지막)에 둡니다. 자동으로 걸러내지 않고 탐지만 합니다 — 05의 다른 검토 테이블들과
# MAGIC 동일한 정책입니다.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.low_salience_review_needed AS
WITH chunk_spread_raw AS (
    SELECT e.class_type, e.name AS raw_name, count(DISTINCT pe.chunk_id) AS n_chunks
    FROM {catalog_name}.{schema_name}.parsed_extractions pe
    LATERAL VIEW explode(pe.parsed.entities) t AS e
    GROUP BY e.class_type, e.name
),
chunk_spread_canon AS (
    SELECT COALESCE(c.canonical_name, cs.raw_name) AS name, cs.class_type,
           sum(cs.n_chunks) AS chunk_spread
    FROM chunk_spread_raw cs
    LEFT JOIN {catalog_name}.{schema_name}.entity_canonicalization c
      ON c.class_type = cs.class_type AND c.raw_name = cs.raw_name
    GROUP BY COALESCE(c.canonical_name, cs.raw_name), cs.class_type
),
degree AS (
    SELECT class_type, name, count(*) AS degree, count(DISTINCT predicate) AS predicate_diversity
    FROM (
        SELECT source_type AS class_type, source_name AS name, predicate
        FROM {catalog_name}.{schema_name}.relationships_resolved
        UNION ALL
        SELECT target_type AS class_type, target_name AS name, predicate
        FROM {catalog_name}.{schema_name}.relationships_resolved
    )
    GROUP BY class_type, name
),
combined AS (
    SELECT er.class_type, er.name,
           COALESCE(d.degree, 0) AS degree,
           COALESCE(d.predicate_diversity, 0) AS predicate_diversity,
           COALESCE(cs.chunk_spread, 0) AS chunk_spread
    FROM {catalog_name}.{schema_name}.entities_resolved er
    LEFT JOIN degree d ON d.class_type = er.class_type AND d.name = er.name
    LEFT JOIN chunk_spread_canon cs ON cs.class_type = er.class_type AND cs.name = er.name
),
with_median AS (
    SELECT *,
           percentile_approx(degree, 0.5) OVER (PARTITION BY class_type) AS class_median_degree,
           percentile_approx(predicate_diversity, 0.5) OVER (PARTITION BY class_type) AS class_median_pred_diversity
    FROM combined
)
SELECT class_type, name, degree, predicate_diversity, chunk_spread,
       class_median_degree, class_median_pred_diversity
FROM with_median
WHERE (class_median_degree >= 3 AND degree <= 1)
   OR (class_median_pred_diversity >= 2 AND predicate_diversity <= 1)
ORDER BY class_type, degree, predicate_diversity
""")

n_low_salience = spark.table(f"{catalog_name}.{schema_name}.low_salience_review_needed").count()
print(f"⚠️ 같은 타입 내에서 유독 고립된(구조적 이상치) 엔티티: {n_low_salience}건")
display(spark.table(f"{catalog_name}.{schema_name}.low_salience_review_needed"))