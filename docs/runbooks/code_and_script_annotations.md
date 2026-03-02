# Code and Script Annotations

이 문서는 최근 추가/수정된 코드와 스크립트의 목적, 입력, 출력, 실패 조건을 주석 형태로 통합 정리합니다.

## 1) End-to-end orchestration

### `scripts/run_all_checks_and_prepare_ui.sh`
- 목적: Rust 검증 + 서비스 smoke + 복합 파이프라인 검증 + Next.js UI 검증을 한 번에 실행.
- 입력: 환경변수 `ROOTSYS_RUN_UI_DEV`(기본 `0`, `1`이면 마지막에 `npm run dev` 실행).
- 동작 순서:
  1. prerequisite 확인(`cargo`, `python3`, `npm`, `docker daemon`)
  2. `cargo fmt --check`, `cargo check`, `cargo test`, `cargo build`
  3. `scripts/run_service_smoke_tests.sh`
  4. `scripts/run_complex_pipeline_checks.sh`
  5. `ui` 의존성 설치 + `typecheck` + `build`
- 실패 조건: 어느 단계에서든 non-zero exit면 즉시 중단(`set -euo pipefail`).

### `scripts/lib/company_config.sh`
- 목적: 기업별 프로파일(`config/companies/*.env`) 로드 + 필수 경로 검증 공통 처리.
- 핵심 함수:
  - `load_company_config(root_dir)`: 프로파일/직접 지정 config를 반영하고 기본값 주입
  - `validate_company_config()`: 계약/인터페이스 필수 파일 존재 여부 검증
- 경로 처리: 상대 경로를 프로젝트 루트 기준 절대 경로로 정규화.

### `scripts/create_company_profile.sh`
- 목적: 신규 제조사 온보딩 시 `config/companies/<name>.env`를 템플릿에서 생성.
- 입력: 프로파일 이름(소문자/숫자/하이픈).
- 출력: 새 회사 전용 `.env` 프로파일 파일.

### `scripts/validate_company_profile.sh`
- 목적: 선택한 프로파일을 로드하고 계약/인터페이스 경로 존재 여부를 실행 전 검증.
- 출력: 활성 config 파일 경로 및 resolved 인터페이스 경로 목록.

## 2) Service smoke tests

### `scripts/run_service_smoke_tests.sh`
- 목적: REST/Postgres/MySQL 실제 서비스 기반 smoke 테스트를 실행하고 `record_id` 정확성 검증.
- 출력 아티팩트:
  - `/tmp/rootsys-smoke/rest.output.jsonl`
  - `/tmp/rootsys-smoke/postgres.output.jsonl`
  - `/tmp/rootsys-smoke/mysql.output.jsonl`
  - `/tmp/rootsys-smoke/merged.db.output.jsonl`
- 의존 리소스:
  - `scripts/smoke/docker-compose.yml`
  - `scripts/smoke/rest_mock_server.py`
  - `tests/fixtures/interfaces/rest.smoke.json`
  - `tests/fixtures/interfaces/postgres.smoke.json`
  - `tests/fixtures/interfaces/mysql.smoke.json`

### `scripts/smoke/docker-compose.yml`
- Postgres 16, MySQL 8.4를 로컬 smoke 테스트용으로 실행.
- 초기 데이터는 SQL init 파일로 주입.

### `scripts/smoke/postgres/init.sql`, `scripts/smoke/mysql/init.sql`
- smoke 검증용 `defect_events` 테이블 생성 + deterministic 샘플 데이터 삽입.

### `scripts/smoke/rest_mock_server.py`
- `/events` 엔드포인트에 고정 JSON 응답 제공.
- REST driver 테스트의 deterministic 입력원 역할.

## 3) Complex checks

### `scripts/run_complex_pipeline_checks.sh`
- 목적: 단순 smoke를 넘어 스케줄링/리플레이/제품흐름/병합 시나리오를 실검증.
- 핵심 검증:
  - interval mode 2회 실행 결과 건수 검증
  - product-flow 아티팩트 생성 검증
  - strict -> sqlite DLQ -> permissive replay 복구 검증
  - multi-source merge + dedupe 검증
- 재실행 안정성:
  - 실행 시작 시 `/tmp/rootsys-complex`를 초기화해 누적 데이터로 인한 오탐 방지.

## 4) UI implementation (Next.js 16)

### `ui/app/page.tsx`
- 목적: smoke/complex 산출물 상태를 대시보드로 표시.
- 표시 정보:
  - artifact 상태(ready/missing/invalid)
  - 레코드 수/유니크 record_id 수
  - source 목록, sample record_id

### `ui/lib/artifacts.ts`
- 목적: JSONL artifact 파일을 읽어 대시보드 데이터 모델로 변환.
- 파싱 정책:
  - 필수 필드(`source`, `record_id`, `ingested_at_unix_ms`) 검증
  - 파일 미존재는 `missing`, 파싱 실패는 `invalid`로 분류

### `ui/app/layout.tsx`, `ui/app/globals.css`
- 목적: 대시보드 기본 레이아웃/테마/반응형 스타일 구성.

### `ui/package.json`, `ui/tsconfig.json`, `ui/next.config.ts`
- 목적: Next.js 16.1.6 + TypeScript 빌드/실행 설정.

## 5) Documentation wiring

### `README.md`
- 실행 명령(서비스 smoke, complex checks, one-shot orchestrator, UI 실행) 안내.

### `docs/runbooks/complex_pipeline_checks.md`
- complex 검증 전용 런북.

### `docs/runbooks/README.md`, `docs/ui/README.md`, `scripts/README.md`
- 새 스크립트/런북/UI 경로 인덱싱.
