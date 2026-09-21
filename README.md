# Ontology_DBX_v2

Databricks 네이티브 AI 함수(`ai_parse_document`, `ai_prep_search`, `ai_query`)만으로 PDF 문서를 **온톨로지 그래프**(엔티티 + 관계)로 변환하는 재사용 가능한 파이프라인입니다. 특정 문서 하나에 맞춰 하드코딩하는 대신, 문서 도메인이 바뀌어도 온톨로지 스키마 자체를 LLM이 매번 새로 제안하도록 설계했습니다.

## 무엇을 만드는가

PDF 원본 하나를 넣으면 다음이 자동 생성됩니다.

- `ontology_entity_types` / `ontology_predicates` — 이 문서에 맞게 LLM이 제안한 엔티티 타입·관계 타입 스키마
- `entities_resolved` — 정규화된 노드 테이블 (`node_id`, `class_type`, `name`)
- `relationships_resolved` — 정규화된 엣지 테이블 (`source_node_id`, `predicate`, `target_node_id`)
- `ontology_predicate_combos` — 실제 데이터에서 관측된 `(source_type, predicate, target_type)` 조합 집계

이 4개 테이블이 그래프 DB 적재(Neo4j, Neptune 등)나 Genie/AI·BI 같은 Text2SQL 레이어의 바로 다음 입력이 됩니다.

## 핵심 설계 포인트

| 문제 | 해결 방식 |
|---|---|
| 문서마다 온톨로지가 다름 | 하드코딩 대신 `03_Ontology_Bootstrap`이 문서 발췌본을 보고 매번 스키마를 새로 제안 |
| 핵심 주제 대상과 비교·참조용 대상이 한 타입에 섞임 | 도메인별 구체적 예시를 프롬프트에 박아넣지 않고, 03에서 매번 동일한 추상 질문으로 LLM이 자기 제안을 재검토(자기 비판) — 도메인이 바뀌어도 같은 프롬프트가 그대로 작동 |
| 청크 경계가 섹션과 안 맞음 | `ai_prep_search` 청크를 그대로 쓰고, 섹션↔청크 커버리지 감사 테이블로 누락을 조기 탐지 |
| 같은 실체가 다른 이름으로 추출됨 (표기 편차) | `05_Canonicalization_Bootstrap`이 `ai_query`로 자동 판단 — 확신도 높은 것만 자동 반영, 애매한 건 검토 테이블에 분리 |
| 같은 이름이 다른 타입으로 추출됨 (타입 충돌) | 05단계에 SQL 전용 탐지 로직 추가 — LLM 호출 없이 항상 작동하는 안전장치 |
| 같은 이름이 매번 일관되게 틀린 타입으로만 추출됨 (타입 충돌 탐지의 사각지대) | `07_Resolve_Relationships` 말미에 SQL 전용 구조적 고립도(salience) 탐지 추가 — 같은 타입 내 다른 엔티티 대비 관계 수·predicate 다양성·등장 청크 수가 유독 적은("고립된") 엔티티를 탐지 |
| 04 추출 시점에 핵심 주제와 배경/참조 대상이 실제로 섞임 | `04_Structured_Extraction`이 청크마다 `chunk_role`(core_narrative/background_reference)을 먼저 판단하고, 03이 만든 `example_instances`를 앵커링 정보로 함께 제공 — 서로 다른 두 문서에서 오염 재현 안 됨을 실측 검증 |
| 관계는 추출됐는데 대응 노드가 없어 조용히 유실됨 | `06_Resolve_Entities`가 관계의 source/target도 노드 후보로 포함 — 무음 손실 원천 차단 |
| 새 문서마다 위젯을 수동 재입력해야 함 | 노트북 간 `%run` 상태 공유를 없애고, 각 노트북이 자신에게 필요한 위젯만 선언 — Databricks Job 파라미터로 완전 자동 주입 가능 |

## 파이프라인 구조

| 노트북 | 역할 |
|---|---|
| `01_Setup` | 카탈로그/스키마/Volume 생성 |
| `02_Parse_And_Chunk` | PDF 파싱 → 섹션 목록 추출 → `ai_prep_search` 청킹 → 커버리지 감사 |
| `03_Ontology_Bootstrap` | 문서 발췌본으로 엔티티 타입/predicate 스키마 자동 제안 (사람 확인 체크포인트 포함) |
| `04_Structured_Extraction` | 확정된 스키마로 청크별 구조화 추출, 청크별 `chunk_role` 판단 포함 (실패 청크 자동 재시도) |
| `05_Canonicalization_Bootstrap` | 표기 편차 자동 정규화 제안 + 타입 충돌 탐지 |
| `06_Resolve_Entities` | 노드 테이블(`entities_resolved`) 생성 |
| `07_Resolve_Relationships` | 엣지 테이블(`relationships_resolved`) 생성 + 관측된 관계 조합 집계 + 구조적 고립도(salience) 탐지 |

## 실행 방법

### A. Databricks Job으로 실행 (권장)

이 저장소의 노트북들은 `Ontology_DBX_v2 Pipeline`이라는 Databricks Job(01→07 태스크 의존관계로 연결됨)으로 이미 구성돼 있습니다. Job 파라미터가 각 태스크의 위젯에 자동 주입되므로, **노트북을 하나도 열지 않고** 파라미터만 채워서 Run 하면 끝까지 자동 실행됩니다. 노트북을 직접 열어 위젯에 입력하는 방식은 Serverless 환경에서 위젯 UI가 가끔 렌더링되지 않는 문제가 있어(브라우저 새로고침으로 대부분 해결되지만), 이 Job 경로가 훨씬 안정적입니다.

#### 1단계 — 원본 PDF 업로드 (Job 실행 전 필수)

Job은 스키마/Volume은 만들어주지만(01_Setup) 파일 업로드까지는 하지 않습니다. 아래 경로에 PDF를 먼저 올려두세요:

```
/Volumes/{catalog_name}/{schema_name}/source_files/
```

예: `/Volumes/kbinsure_wksp/ontology_dbx_test4/source_files/my_document.pdf`

- Catalog Explorer(왼쪽 사이드바 → Catalog)에서 드래그앤드롭으로 업로드하거나,
- CLI: `databricks fs cp <로컬경로>.pdf "dbfs:/Volumes/{catalog}/{schema}/source_files/" --profile <프로필명>`

스키마가 아직 없어도 됩니다 — Job의 첫 태스크(`01_Setup`)가 `CREATE SCHEMA/VOLUME IF NOT EXISTS`로 알아서 만듭니다. 다만 **Volume 자체가 없으면 업로드할 곳이 없으므로**, 완전히 새 스키마라면 먼저 SQL 에디터에서 아래 두 줄만 실행해 스키마/Volume을 만든 뒤 업로드하세요:
```sql
CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>;
CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.source_files;
```

#### 2단계 — Job 찾기 & 파라미터 채워서 실행

1. 왼쪽 사이드바 **Workflows** → **Jobs & Pipelines** → `Ontology_DBX_v2 Pipeline` 클릭
2. 우측 상단 **Run now** 버튼 옆 드롭다운(▼) → **"Run now with different parameters"** 선택
3. 아래 4개 파라미터 입력:

| 파라미터 | 설명 | 예시 |
|---|---|---|
| `catalog_name` | Unity Catalog 카탈로그명 | `kbinsure_wksp` |
| `schema_name` | 이 문서 전용 격리 스키마명 (기존 데이터와 안 섞이게 매 문서마다 새로 만드는 걸 권장) | `ontology_dbx_test4` |
| `document_domain_hint` | 문서 도메인을 LLM에게 설명하는 한두 문장 — 03 온톨로지 제안 프롬프트에 직접 들어가므로 구체적일수록 좋음 | `인수금융 Information Memorandum(IM) - OO산업 OO회사 인수금융` |
| `source_pdf_glob` | Volume 내 원본 파일 패턴 | `*.pdf` (기본값 그대로 두면 됨) |

4. **Run** 클릭

#### 3단계 — 진행 상황 모니터링

Job 상세 화면의 **Runs** 탭에서 방금 시작한 run을 클릭하면 01→07 태스크 그래프가 보입니다(회색=대기, 파랑=실행중, 초록=성공, 빨강=실패). 문서 규모에 따라 보통 5~15분 정도 걸립니다(04 구조화 추출이 청크 수만큼 `ai_query`를 호출해서 가장 오래 걸립니다). 개별 태스크 박스를 클릭하면 해당 노트북의 실제 출력(로그, 생성된 테이블 행 수 등)을 볼 수 있습니다.

#### 4단계 — 실패 시 재실행 (Repair run)

특정 태스크가 실패해도 처음부터 다시 돌릴 필요 없습니다. Run 상세 화면 우측 상단 **Repair run** 클릭 → 실패한 태스크와 그 이후 태스크만 재실행됩니다(성공한 앞 단계 결과물은 그대로 유지). CLI로 하려면:
```bash
databricks jobs repair-run <run_id> --json '{
  "run_id": <run_id>,
  "latest_repair_id": <직전 repair_id, 없으면 생략>,
  "rerun_all_failed_tasks": true,
  "job_parameters": {"catalog_name": "...", "schema_name": "...", "document_domain_hint": "...", "source_pdf_glob": "*.pdf"}
}' --profile <프로필명>
```
⚠️ `job_parameters`를 생략하면 Job 기본값(대부분 빈 문자열)으로 재실행되어 위젯 assert에서 바로 실패합니다 — repair 시에도 항상 파라미터를 다시 넣어주세요.

#### 5단계 — 완료 후 확인할 것

전부 성공했다고 바로 신뢰하지 말고, 아래 테이블들을 SQL 에디터나 Catalog Explorer에서 한 번 훑어보는 걸 권장합니다(전부 "자동 반영 안 되고 검토만 필요한" 소프트 게이트 결과물입니다):

| 테이블 | 확인할 것 |
|---|---|
| `section_coverage_audit` | `covering_chunks=0`인 행이 있으면 실제 누락인지 오탐인지 원문 대조 |
| `canonicalization_review_needed` | 05가 확신 없어 자동 반영 안 한 동의어 후보 |
| `type_conflict_review_needed` | 같은 이름이 다른 타입으로도 뽑힌 경우 |
| `low_salience_review_needed` | 같은 타입 내에서 유독 고립된(관계 수·청크 등장 수가 적은) 엔티티 — 타입 오분류 의심 |

### B. CLI로 트리거 (스크립팅/자동화용)

```bash
databricks api post /api/2.1/jobs/run-now --profile <프로필명> --json '{
  "job_id": <job_id>,
  "job_parameters": {
    "catalog_name": "kbinsure_wksp",
    "schema_name": "ontology_dbx_test4",
    "document_domain_hint": "...",
    "source_pdf_glob": "*.pdf"
  }
}'
```
`job_id`는 Job 상세 화면 URL(`.../jobs/<job_id>`) 또는 `databricks jobs list`로 확인할 수 있습니다.

### C. 노트북 개별 실행 (디버깅용)

각 노트북을 직접 열면 그 노트북에 필요한 위젯만 상단에 나타납니다(안 보이면 브라우저 새로고침, 그래도 안 되면 `dbutils.widgets.text(name, "실제값")`을 별도 셀에서 먼저 실행해 값을 채우는 우회법이 있습니다). 값을 채우고 01부터 순서대로 실행하세요. `03_Ontology_Bootstrap`의 사람 확인 체크포인트(제안된 스키마 검토, 타입 혼동 위험 자기비판 결과)는 소프트 게이트이므로, 품질을 확인하려면 Run All로 그냥 넘기지 말고 실제로 멈춰서 읽어보는 걸 권장합니다.

## 알려진 제한사항

- `03`의 온톨로지 제안은 LLM 호출이라 같은 문서로 재실행해도 결과가 달라질 수 있습니다 (비결정성).
- `entity_canonicalization`/`type_conflict_review_needed`/`low_salience_review_needed`가 확신도 낮은 항목을 자동 반영하지 않는 건 의도된 동작입니다 — 필요 시 사람이 직접 검토 후 반영해야 합니다.
- `chunk_role` 기반 오염 예방은 서로 다른 두 인수금융 IM 문서에서 재현 검증했지만, 아직 같은 도메인 두 건일 뿐입니다 — 완전히 다른 도메인 문서에서도 유지되는지는 계속 지켜볼 필요가 있습니다.
- 개인정보(담당자 실명·연락처 등)는 추출 규칙에서 명시적으로 제외됩니다.
