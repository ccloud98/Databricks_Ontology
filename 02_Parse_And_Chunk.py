# Databricks notebook source
# MAGIC %md
# MAGIC ## 02_Parse_And_Chunk
# MAGIC PDF 파싱 → 요소 추출(섹션 목록 확보용) → `ai_prep_search` 기반 청킹(방안 A) → 커버리지 감사
# MAGIC
# MAGIC v2 변경점: `%run ./01_Setup` 대신 이 노트북이 필요한 위젯(catalog_name/schema_name/source_pdf_glob)을
# MAGIC 직접 선언합니다. Job으로 실행하면 Job 파라미터가 자동으로 채우고, 단독 실행 시에도 이 노트북 자체에
# MAGIC 위젯 UI가 정상적으로 나타납니다.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "카탈로그명")
dbutils.widgets.text("schema_name", "", "스키마명 (프로젝트 전용 격리)")
dbutils.widgets.text("source_pdf_glob", "*.pdf", "Volume 내 원본 문서 파일 패턴")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
source_pdf_glob = dbutils.widgets.get("source_pdf_glob")

assert catalog_name and schema_name, "catalog_name / schema_name 위젯을 모두 채워주세요."

files_path = f"/Volumes/{catalog_name}/{schema_name}/source_files"

# COMMAND ----------

from pyspark.sql.functions import expr, col, explode

raw_docs = spark.read.format("binaryFile").load(f"{files_path}/{source_pdf_glob}")
parsed_docs = raw_docs.withColumn("parsed", expr("ai_parse_document(content)"))
parsed_docs.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.raw_parsed_documents")
print(f"파싱된 문서 수: {parsed_docs.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 요소 추출 — 섹션 제목 목록(section_master) 확보용
# MAGIC `ai_prep_search`의 청킹 결과와는 별개로, "이 문서에 실제로 어떤 섹션들이 있었는가"라는 기준(ground truth)을
# MAGIC 먼저 확보해야 아래 커버리지 감사가 가능합니다.

# COMMAND ----------

elements_df = (
    spark.table(f"{catalog_name}.{schema_name}.raw_parsed_documents")
    .select("path", expr("""
        variant_get(parsed, '$.document.elements',
            'ARRAY<STRUCT<id:INT, type:STRING, content:STRING>>')
    """).alias("elements"))
    .withColumn("element", explode("elements"))
    .select("path", "element.*")
)
elements_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.parsed_elements")

section_master_df = (
    spark.table(f"{catalog_name}.{schema_name}.parsed_elements")
    .filter(col("type").isin("section_header", "title"))
    .select("path", "content")
    .distinct()
    .withColumnRenamed("content", "section_title")
)
section_master_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.section_master")
print(f"원본 섹션 수: {section_master_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `ai_prep_search` 기반 청킹 (방안 A)
# MAGIC 섹션 경계를 강제하지 않고 `ai_prep_search`의 청크를 그대로 채택합니다 — 검증 결과 섹션 경계가 유지되지
# MAGIC 않는다는 게 확인됐지만(청크 하나가 여러 섹션을 뭉치기도 함), 그 대가로 청크 수가 줄어 4단계 호출 비용이
# MAGIC 절감됩니다. 대신 `chunk_to_embed`에 담긴 "Sections: ..." 메타데이터를 파싱해 뒤에서 커버리지를 감사합니다.
# MAGIC
# MAGIC 4단계 추출에는 **요약본(`chunk_to_embed`)이 아니라 원문(`chunk_to_retrieve`)을 사용**합니다 — 추출은
# MAGIC LLM이 만든 요약이 아니라 원본 텍스트를 근거로 해야 하기 때문입니다.

# COMMAND ----------

chunks_raw_df = (
    spark.table(f"{catalog_name}.{schema_name}.raw_parsed_documents")
    .select("path", expr("""
        variant_get(ai_prep_search(parsed), '$.document.contents',
            'ARRAY<STRUCT<chunk_id:STRING, chunk_position:INT, chunk_to_retrieve:STRING, chunk_to_embed:STRING>>')
    """).alias("chunks"))
    .withColumn("c", explode("chunks"))
    .select(
        "path",
        col("c.chunk_id").alias("chunk_id"),
        col("c.chunk_position").alias("chunk_position"),
        col("c.chunk_to_retrieve").alias("chunk_text"),
        col("c.chunk_to_embed").alias("chunk_to_embed"),
    )
    .withColumn("sections_covered", expr(r"regexp_extract(chunk_to_embed, 'Sections: ([^\\n]*)', 1)"))
)
chunks_raw_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.document_chunks")
print(f"생성된 청크 수: {chunks_raw_df.count()} (원본 섹션 수 {section_master_df.count()}개 대비)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 커버리지 감사 — 어느 청크에도 안 걸린 섹션이 있는지 확인
# MAGIC 결과에 행이 있으면, 그 섹션은 4단계 추출에 전혀 들어가지 못했다는 뜻이므로 다음 단계로 넘어가기 전에
# MAGIC 반드시 확인해야 합니다(과거 "섹션 50 유실" 같은 사고를 청킹 직후 단계에서 조기 발견하기 위한 안전장치).
# MAGIC
# MAGIC ⚠️ 이 매칭은 문자열 완전일치 기준입니다 — `ai_prep_search`가 생성한 "Sections:" 표기가 원본 섹션 제목과
# MAGIC 미세하게(공백/구두점 등) 다르면 오탐(실제로는 커버되었는데 누락으로 표시)이 발생할 수 있으니, 결과가
# MAGIC 나오면 실제로 해당 섹션 내용이 어느 청크에도 없는지 `document_chunks.chunk_text`를 직접 검색해 한 번 더
# MAGIC 확인하는 것을 권장합니다. (실측 사례: 코아비스 IM 검증 런에서 20개 섹션이 이 기준으로 "누락"으로 잡혔으나,
# MAGIC 스팟체크 결과 대부분 반복 헤더/오탐이었고 실제 내용은 청크에 존재했습니다.)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.section_coverage_audit AS
WITH chunk_sections AS (
  SELECT chunk_id, explode(split(sections_covered, '; ')) AS section_title
  FROM {catalog_name}.{schema_name}.document_chunks
  WHERE sections_covered != ''
)
SELECT sm.section_title, count(cs.chunk_id) AS covering_chunks
FROM {catalog_name}.{schema_name}.section_master sm
LEFT JOIN chunk_sections cs ON sm.section_title = cs.section_title
GROUP BY sm.section_title
""")

missing = spark.sql(f"""
    SELECT * FROM {catalog_name}.{schema_name}.section_coverage_audit WHERE covering_chunks = 0
""")
missing_count = missing.count()
if missing_count > 0:
    print(f"⚠️ {missing_count}개 섹션이 문자열 완전일치 기준으로 어느 청크에도 걸리지 않았습니다. 아래 목록을 확인하세요.")
    display(missing)
else:
    print("✅ 모든 섹션이 최소 1개 이상의 청크에 (문자열 완전일치 기준으로) 포함되어 있습니다.")