# ROOTSYS (한국어 가이드)

현재 구현 상태 기준 한국어 진입 가이드입니다.

## 구현된 범위
- Rust 기반 데이터 통합 런타임 (`shell`, `runtime`, `drivers`, `fabric`)
- 실서비스 기반 smoke 테스트 (REST/Postgres/MySQL)
- 복합 파이프라인 검증 (interval stream, product flow, DLQ replay, merge)
- 기업별 프로파일 설정 (`config/companies/*.env`)
- Next.js 16 런타임 대시보드 (`ui/`)

## 주요 실행 명령
- 전체 원샷 실행:
```bash
bash scripts/run_all_checks_and_prepare_ui.sh default
```
- 고객사 프로파일 생성:
```bash
bash scripts/create_company_profile.sh <company-name>
```
- 고객사 프로파일 검증:
```bash
bash scripts/validate_company_profile.sh <company-name>
```

## 대용량 테스트 예시
```bash
ROOTSYS_SMOKE_DB_COUNT=500 \
ROOTSYS_SMOKE_REST_COUNT=500 \
ROOTSYS_COMPLEX_STREAM_RECORD_COUNT=1000 \
ROOTSYS_COMPLEX_REPLAY_INPUT_COUNT=200 \
bash scripts/run_all_checks_and_prepare_ui.sh default
```

## 상세 문서 위치
- 코어 사용법/구조: `README.md`
- 스크립트 목록: `scripts/README.md`
- 기업 프로파일: `config/companies/README.md`
- 런북 인덱스: `docs/runbooks/README.md`
- UI 앱: `ui/README.md`
