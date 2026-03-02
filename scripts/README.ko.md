# scripts (한국어)

로컬 실행, 검증, 고객사 온보딩용 스크립트 진입점입니다.

## 핵심 스크립트
- `scripts/run_all_checks_and_prepare_ui.sh`: 원샷 전체 플로우 (Rust 게이트 + smoke + complex + UI 빌드)
- `scripts/run_service_smoke_tests.sh`: 실서비스 기반 smoke 검증 (REST/Postgres/MySQL)
- `scripts/run_complex_pipeline_checks.sh`: 심화 검증 (스케줄/product flow/replay/merge)
- `scripts/run_local_mvp_bootstrap.sh`: 기본 로컬 MVP 체인
- `scripts/create_company_profile.sh`: 신규 고객사 프로파일 env 생성
- `scripts/validate_company_profile.sh`: 선택 프로파일 경로/설정 검증
- `scripts/create_sample_dbs.py`: sqlite fixture 데이터 생성(건수 조절 가능)

## 공통 설정 로더
- 실행 스크립트는 `scripts/lib/company_config.sh`를 공통 사용
- 프로파일 로드 + 기본값 주입 + 필수 파일/숫자 설정 검증 수행

## 권장 실행 순서
```bash
bash scripts/create_company_profile.sh hanul-motors
bash scripts/validate_company_profile.sh hanul-motors
bash scripts/run_all_checks_and_prepare_ui.sh hanul-motors
```
