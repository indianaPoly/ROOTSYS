# runbooks (한국어)

ROOTSYS 런타임 실행/운영/릴리스 준비를 위한 런북 인덱스입니다.

## 주요 런북
- `service_smoke_tests.md`: 실서비스 기반 smoke 실행/검증
- `complex_pipeline_checks.md`: 심화 런타임 검증 (interval/product/replay/merge)
- `company_profile_configuration.md`: 고객사 프로파일 기반 실행 설정
- `code_and_script_annotations.md`: 스크립트/코드 역할 매핑 문서
- `integration_definition_of_done.md`: 통합 완료 기준(DoD)
- `idempotency_dedupe_strategy.md`: 중복제거/멱등 운영 전략

## 권장 진행 순서
1. 프로파일 설정 (`company_profile_configuration.md`)
2. smoke 실행 (`service_smoke_tests.md`)
3. complex 실행 (`complex_pipeline_checks.md`)
4. 운영/릴리스 기준 점검
