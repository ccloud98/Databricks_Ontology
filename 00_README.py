# Databricks notebook source
# MAGIC %md
# MAGIC # Ontology_DBX_v2 — 자동화 강화 버전
# MAGIC
# MAGIC `/Workspace/Ontology_DBX`(v1)를 실제 신규 도메인 문서(코아비스 인수금융 IM, `ontology_dbx_test2` 스키마)에
# MAGIC 돌려본 결과 드러난 두 가지 구조적 문제를 반영해 다시 구현한 버전입니다.
# MAGIC
# MAGIC ## v1 대비 달라진 점
# MAGIC
# MAGIC ### 1. 동의어(표기 편차) 처리 자동화
# MAGIC v1은 `05_Resolve_Entities`의 "① 표기 편차 확인 → ② 사람이 직접 INSERT"가 마크다운 안내문일 뿐 실제로
# MAGIC 실행을 막지 않아서, 사람이 그냥 넘어가면 `entity_canonicalization`이 계속 빈 테이블로 남았습니다.
# MAGIC 실측 결과(코아비스 IM) 코아비스 자신이 `대상회사`/`차주`/`코아비스`/`대상회사 (PortfolioCompany)` 4갈래로
# MAGIC 쪼개졌습니다. v2는 새 노트북 `05_Canonicalization_Bootstrap`을 추가해 `ai_query`가 표기 편차를 자동으로
# MAGIC 판단하고, 확신도가 높은 것만 자동으로 `entity_canonicalization`에 반영합니다. 애매한 것은 자동 반영하지
# MAGIC 않고 `canonicalization_review_needed` 테이블에 후보로만 남겨, 사람이 볼 범위를 "전체 표기 편차"에서
# MAGIC "애매한 것만"으로 좁힙니다. (주의: 이 단계는 "같은 타입 안의 동의어"만 해결합니다. 애초에 다른 실체가
# MAGIC 잘못된 타입으로 추출된 문제(예: Sponsor의 과거 투자사가 PortfolioCompany로 섞여 들어간 것)는 별도 이슈로,
# MAGIC 이 자동화로 해결되지 않습니다 — 03_Ontology_Bootstrap의 타입 정의를 더 좁히거나 04 이후 별도 재검증이 필요)
# MAGIC
# MAGIC ### 2. `%run` 기반 설정 공유 제거
# MAGIC v1은 모든 노트북이 `%run ./01_Config`로 `catalog_name`/`schema_name`/... 을 물려받는 구조였는데,
# MAGIC Serverless 환경에서 `%run`으로 호출된 하위 노트북의 위젯이 상위 노트북 UI에 렌더링되지 않아, 매 노트북
# MAGIC 진입 시마다 위젯 재입력이 필요했고(자동화와 상충), 이를 우회하려면 `%run` 직전에 위젯 값을 하드코딩하는
# MAGIC 코드 스니펫을 매번 손으로 넣어야 했습니다. v2는 `%run`을 완전히 제거하고, **노트북마다 자신에게 필요한
# MAGIC 위젯만 직접 선언**합니다. 이렇게 하면:
# MAGIC - Databricks **Job**으로 여러 노트북을 태스크로 묶어 실행할 경우, Job 파라미터가 각 태스크의 동일 이름
# MAGIC   위젯에 자동 주입되어 **사람이 값을 입력할 필요가 전혀 없습니다** (진짜 "자동화 경로").
# MAGIC - 개별 노트북을 단독으로 열어 디버깅할 때도, 그 노트북 자체에 위젯 UI가 정상적으로 나타납니다.
# MAGIC
# MAGIC 이 두 변경을 반영해 실제로 `Ontology_DBX_v2 Pipeline`이라는 Databricks Job도 함께 만들어뒀습니다 —
# MAGIC Job 파라미터(`catalog_name`/`schema_name`/`document_domain_hint`/`source_pdf_glob`)만 채워서 Run을
# MAGIC 누르면 01→07이 전부 자동 실행됩니다.
# MAGIC
# MAGIC ## 노트북 구성 (v1 → v2 매핑)
# MAGIC
# MAGIC | v2 | v1 | 내용 | 비고 |
# MAGIC |---|---|---|---|
# MAGIC | `01_Setup` | `01_Config` | 카탈로그/스키마/볼륨 생성 | 더 이상 Python 상태를 다른 노트북에 넘기지 않음 (역할이 인프라 생성으로 축소) |
# MAGIC | `02_Parse_And_Chunk` | `02_Parse_And_Chunk` | PDF 파싱 → 청킹 → 커버리지 감사 | 위젯 직접 선언 |
# MAGIC | `03_Ontology_Bootstrap` | `03_Ontology_Bootstrap` | 온톨로지 자동 제안 + 사람 확인 체크포인트 | 위젯 직접 선언 |
# MAGIC | `04_Structured_Extraction` | `04_Structured_Extraction` | 청크별 구조화 추출 | 위젯 직접 선언, `EXTRACTION_FIXED_RULES` 이 노트북에 내장 |
# MAGIC | `05_Canonicalization_Bootstrap` | *(신규)* | 표기 편차 자동 판단 → `entity_canonicalization` 자동 반영 | ai_query 기반, high/low 확신도 분리 |
# MAGIC | `06_Resolve_Entities` | `05_Resolve_Entities` | entities_resolved 생성 | 이제 entity_canonicalization이 05에서 이미 채워진 상태로 진입 |
# MAGIC | `07_Resolve_Relationships` | `06_Resolve_Relationships` | relationships_resolved 생성 | 로직 동일 |
# MAGIC
# MAGIC ## 사용법
# MAGIC **A. Job으로 실행(권장, 완전 자동화)**: `Ontology_DBX_v2 Pipeline` Job을 열어 파라미터
# MAGIC (`catalog_name`/`schema_name`/`document_domain_hint`/`source_pdf_glob`) 입력 후 Run. PDF는 미리
# MAGIC `/Volumes/{catalog}/{schema}/source_files`에 업로드되어 있어야 합니다(01_Setup이 스키마/볼륨만 만들고
# MAGIC 파일 업로드까지 자동으로 하지는 않습니다).
# MAGIC
# MAGIC **B. 노트북을 하나씩 수동 실행(디버깅용)**: 각 노트북을 열면 그 노트북에 필요한 위젯만 나타납니다 —
# MAGIC 값을 채우고 01→07 순서로 실행하세요. `03_Ontology_Bootstrap`의 사람 확인 체크포인트(제안된 엔티티
# MAGIC 타입/predicate 검토)는 v1과 동일하게 소프트 게이트입니다 — 이번 변경 범위에는 포함되지 않았습니다.