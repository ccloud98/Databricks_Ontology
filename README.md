# Ontology_DBX_v2

Databricks 네이티브 AI 함수(`ai_parse_document`, `ai_prep_search`, `ai_query`)만으로 PDF 문서를 **온톨로지 그래프**(엔티티 + 관계)로 변환하는 재사용 가능한 파이프라인입니다. 특정 문서 하나에 맞춰 하드코딩하는 대신, 문서 도메인이 바뀌어도 온톨로지 스키마 자체를 LLM이 매번 새로 제안하도록 설계했습니다.

## 무엇을 만드는가

PDF 원본 하나를 넣으면 다음이 자동 생성됩니다.

- `ontology_entity_types` / `ontology_predicates` — 이 문서에 맞게 LLM이 제안한 엔티티 타입·관계 타입 스키마
- `entities_resolved` — 정규화된 노드 테이블 (`node_id`, `class_type`, `name`)
- `relationships_resolved` — 정규화된 엣지 테이블 (`source_node_id`, `predicate`, `target_node_id`)
- `ontology_predicate_combos` — 실제 데이터에서 관측된 `(source_type, predicate, target_type)` 조합 집계

이 4개 테이블이 그래프 DB 적재(Neo4j, Neptune 등)나 Genie/AI·BI 같은 텍스트-투-쿼리 레이어의 바로 다음 입력이 됩니다.

## 핵심 설계 포인트

| 문제 | 해결 방식 |
|---|---|
| 문서마다 온톨로지가 다름 | 하드코딩 대신 `03_Ontology_Bootstrap`이 문서 발췌본을 보고 매번 스키마를 새로 제안 |
| 청크 경계가 섹션과 안 맞음 | `ai_prep_search` 청크를 그대로 쓰고, 섹션↔청크 커버리지 감사 테이블로 누락을 조기 탐지 |
| 같은 실체가 다른 이름으로 추출됨 (표기 편차) | `05_Canonicalization_Bootstrap`이 `ai_query`로 자동 판단 — 확신도 높은 것만 자동 반영, 애매한 건 검토 테이블에 분리 |
| 같은 이름이 다른 타입으로 추출됨 (타입 충돌) | 05단계에 SQL 전용 탐지 로직 추가 — LLM 호출 없이 항상 작동하는 안전장치 |
| 관계는 추출됐는데 대응 노드가 없어 조용히 유실됨 | `06_Resolve_Entities`가 관계의 source/target도 노드 후보로 포함 — 무음 손실 원천 차단 |
| 새 문서마다 위젯을 수동 재입력해야 함 | 노트북 간 `%run` 상태 공유를 없애고, 각 노트북이 자신에게 필요한 위젯만 선언 — Databricks Job 파라미터로 완전 자동 주입 가능 |

## 파이프라인 구조

| 노트북 | 역할 |
|---|---|
| `01_Setup` | 카탈로그/스키마/Volume 생성 |
| `02_Parse_And_Chunk` | PDF 파싱 → 섹션 목록 추출 → `ai_prep_search` 청킹 → 커버리지 감사 |
| `03_Ontology_Bootstrap` | 문서 발췌본으로 엔티티 타입/predicate 스키마 자동 제안 (사람 확인 체크포인트 포함) |
| `04_Structured_Extraction` | 확정된 스키마로 청크별 구조화 추출 (실패 청크 자동 재시도) |
| `05_Canonicalization_Bootstrap` | 표기 편차 자동 정규화 제안 + 타입 충돌 탐지 |
| `06_Resolve_Entities` | 노드 테이블(`entities_resolved`) 생성 |
| `07_Resolve_Relationships` | 엣지 테이블(`relationships_resolved`) 생성 + 관측된 관계 조합 집계 |

## 실행 방법

**Job으로 실행 (권장)** — `Ontology_DBX_v2 Pipeline` Job에 파라미터 4개(`catalog_name`, `schema_name`, `document_domain_hint`, `source_pdf_glob`)만 채우고 Run. PDF는 `/Volumes/{catalog}/{schema}/source_files`에 미리 업로드되어 있어야 합니다. 01→07이 사람 개입 없이 순차 실행됩니다.

**노트북 개별 실행 (디버깅용)** — 각 노트북을 열면 그 노트북에 필요한 위젯만 나타납니다. 값을 채우고 01부터 순서대로 실행하세요. `03_Ontology_Bootstrap`의 사람 확인 체크포인트(제안된 스키마 검토)는 소프트 게이트이므로, 품질을 확인하려면 실제로 멈춰서 읽어보는 걸 권장합니다.

## 알려진 제한사항

- `03`의 온톨로지 제안은 LLM 호출이라 같은 문서로 재실행해도 결과가 달라질 수 있습니다 (비결정성).
- `entity_canonicalization`/`type_conflict_review_needed`가 확신도 낮은 항목을 자동 반영하지 않는 건 의도된 동작입니다 — 필요 시 사람이 직접 검토 후 반영해야 합니다.
- 개인정보(담당자 실명·연락처 등)는 추출 규칙에서 명시적으로 제외됩니다.
