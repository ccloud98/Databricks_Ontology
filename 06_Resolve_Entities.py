# Databricks notebook source
# MAGIC %md
# MAGIC ## 06_Resolve_Entities
# MAGIC `entities_resolved` 생성. v2부터는 `entity_canonicalization`이 05_Canonicalization_Bootstrap에서
# MAGIC (확신도 high인 것만) 자동으로 채워진 상태로 이 노트북에 진입합니다. 이 노트북은 그 결과를 그대로
# MAGIC 반영해 `entities_resolved`를 만듭니다.
# MAGIC
# MAGIC 05가 자동 반영하지 않은 애매한 케이스는 `canonicalization_review_needed`에 남아있습니다 — 필요하면
# MAGIC 아래 ①에서 다시 한번 확인하고, ②에서 직접 승인 규칙을 추가한 뒤 이 노트북을 재실행하세요. v1과 달리
# MAGIC 이건 이제 "반드시 해야 하는 단계"가 아니라 "선택적 추가 보정"입니다 — 05가 이미 표기 편차 대부분을
# MAGIC 처리했기 때문입니다.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "카탈로그명")
dbutils.widgets.text("schema_name", "", "스키마명 (프로젝트 전용 격리)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

assert catalog_name and schema_name, "catalog_name / schema_name 위젯을 모두 채워주세요."

# COMMAND ----------

# MAGIC %md
# MAGIC ### ① 남은 표기 편차 확인 (선택)
# MAGIC 05가 이미 high 확신 그룹은 반영했으므로, 여기 남은 건 05가 애매하다고 판단해 자동 반영을 보류한
# MAGIC 것들 위주입니다. `canonicalization_review_needed`도 함께 참고하세요.

# COMMAND ----------

display(spark.sql(f"""
    WITH raw_ents AS (
        SELECT explode(parsed.entities) AS e
        FROM {catalog_name}.{schema_name}.parsed_extractions
    ),
    canon AS (
        SELECT e.class_type, COALESCE(c.canonical_name, e.name) AS name
        FROM raw_ents e
        LEFT JOIN {catalog_name}.{schema_name}.entity_canonicalization c
          ON c.class_type = e.class_type AND c.raw_name = e.name
    )
    SELECT class_type, collect_set(name) AS distinct_names, count(DISTINCT name) AS n
    FROM canon
    GROUP BY class_type
    ORDER BY n DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ② (선택) 남은 편차에 대해 사람이 직접 규칙 추가
# MAGIC `canonicalization_review_needed`를 보고 실제로 동일 실체라고 판단되면 아래처럼 추가하세요.
# MAGIC ```python
# MAGIC # spark.sql(f"""
# MAGIC #     INSERT INTO {catalog_name}.{schema_name}.entity_canonicalization VALUES
# MAGIC #     ('ClassType', '원본표기', '대표표기')
# MAGIC # """)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### ③ entities_resolved 생성
# MAGIC explode → `entity_canonicalization` 좌조인으로 표준명칭 치환(규칙 없으면 원본 이름 그대로 사용) →
# MAGIC `sha2(class_type|name)` 해시로 결정론적 `node_id` 부여.
# MAGIC
# MAGIC **v2 추가 변경 (INNER JOIN 무음 손실 대응)**: 노드 후보를 `parsed.entities`뿐 아니라
# MAGIC `parsed.relationships`의 source/target에서도 가져옵니다. 04는 청크별로 독립 호출되기 때문에, LLM이
# MAGIC 관계의 source/target으로는 이름을 썼지만 같은 청크의 `entities` 배열에는 그 이름을 별도로 안 뽑는
# MAGIC 경우가 실측으로 확인됐습니다(코아비스 IM 검증 런 기준 관계의 2.8~14.8%). 예전에는 그 이름이
# MAGIC entities_resolved에 없어서 07의 관계 조인이 해당 관계를 조용히 버렸습니다. 이제는 관계에 등장한
# MAGIC 것만으로도 노드가 만들어지므로 그 손실이 구조적으로 발생하지 않습니다.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.entities_resolved AS
WITH entity_names AS (
  SELECT DISTINCT e.class_type, e.name AS raw_name
  FROM {catalog_name}.{schema_name}.parsed_extractions
  LATERAL VIEW explode(parsed.entities) t AS e
),
relationship_endpoints AS (
  SELECT r.source_type AS class_type, r.source_name AS raw_name
  FROM {catalog_name}.{schema_name}.parsed_extractions
  LATERAL VIEW explode(parsed.relationships) t AS r
  UNION
  SELECT r.target_type AS class_type, r.target_name AS raw_name
  FROM {catalog_name}.{schema_name}.parsed_extractions
  LATERAL VIEW explode(parsed.relationships) t AS r
),
raw_ents AS (
  SELECT DISTINCT class_type, raw_name FROM entity_names
  UNION
  SELECT DISTINCT class_type, raw_name FROM relationship_endpoints
),
canon AS (
  SELECT r.class_type,
         COALESCE(c.canonical_name, r.raw_name) AS name
  FROM raw_ents r
  LEFT JOIN {catalog_name}.{schema_name}.entity_canonicalization c
    ON c.class_type = r.class_type AND c.raw_name = r.raw_name
)
SELECT DISTINCT class_type, name, sha2(concat(class_type, '|', name), 256) AS node_id
FROM canon
""")

n = spark.table(f"{catalog_name}.{schema_name}.entities_resolved").count()

n_implied_only = spark.sql(f"""
    WITH exploded_entities AS (
        SELECT e.class_type, e.name AS raw_name
        FROM {catalog_name}.{schema_name}.parsed_extractions
        LATERAL VIEW explode(parsed.entities) t AS e
    ),
    from_entities AS (
        SELECT DISTINCT ee.class_type,
               COALESCE(c.canonical_name, ee.raw_name) AS name
        FROM exploded_entities ee
        LEFT JOIN {catalog_name}.{schema_name}.entity_canonicalization c
          ON c.class_type = ee.class_type AND c.raw_name = ee.raw_name
    )
    SELECT count(*) AS n FROM (
        SELECT class_type, name FROM {catalog_name}.{schema_name}.entities_resolved
        EXCEPT
        SELECT class_type, name FROM from_entities
    )
""").collect()[0].n

print(f"✅ entities_resolved 생성 완료: {n}개 노드 "
      f"(관계에서만 발견되어 보강된 노드 {n_implied_only}건 포함 — 이만큼이 07단계 무음 손실을 예방합니다)")