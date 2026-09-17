# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## 01_Setup
# MAGIC 카탈로그/스키마/볼륨을 생성합니다. v1의 `01_Config`와 달리 다른 노트북에 `%run`으로 Python 상태를
# MAGIC 넘기는 역할은 하지 않습니다 — 순수 인프라 준비 전용이며, 각 노트북은 자신에게 필요한 위젯을 스스로
# MAGIC 선언합니다(Databricks Job으로 묶어 실행하면 Job 파라미터가 자동으로 채워줍니다).

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "카탈로그명")
dbutils.widgets.text("schema_name", "", "스키마명 (프로젝트 전용 격리)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

assert catalog_name and schema_name, "catalog_name / schema_name 위젯을 모두 채워주세요."

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog_name}`.`{schema_name}`.source_files")
files_path = f"/Volumes/{catalog_name}/{schema_name}/source_files"
print(f"✅ 스키마/볼륨 준비 완료. 원본 문서를 이 경로에 업로드하세요: {files_path}")