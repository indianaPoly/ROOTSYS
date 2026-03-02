# Company Profile Configuration

제조사별 환경에 맞춰 실행 전 구성을 분리하기 위한 프로파일 기반 설정 방법입니다.

## 프로파일 파일 위치
- `config/companies/<profile>.env`

기본 제공:
- `config/companies/default.env`
- `config/companies/acme.sample.env`
- `config/companies/first-customer.sample.env`

## 신규 고객 프로파일 생성

```bash
bash scripts/create_company_profile.sh <company-name>
```

예시:
```bash
bash scripts/create_company_profile.sh hanul-motors
```

생성 파일:
- `config/companies/hanul-motors.env`

프로파일 수정 후 검증:
```bash
bash scripts/validate_company_profile.sh hanul-motors
```

## 설정 항목
- 계약 레지스트리: `ROOTSYS_CONTRACT_REGISTRY`
- 인터페이스 경로:
  - `ROOTSYS_INTERFACE_MES`
  - `ROOTSYS_INTERFACE_QMS`
  - `ROOTSYS_INTERFACE_STREAM`
  - `ROOTSYS_INTERFACE_REST_SMOKE`
  - `ROOTSYS_INTERFACE_POSTGRES_SMOKE`
  - `ROOTSYS_INTERFACE_MYSQL_SMOKE`
- smoke 기대 `record_id`:
  - `ROOTSYS_SMOKE_EXPECT_REST_IDS`
  - `ROOTSYS_SMOKE_EXPECT_POSTGRES_IDS`
  - `ROOTSYS_SMOKE_EXPECT_MYSQL_IDS`
- complex replay 기본 인터페이스 식별자:
  - `ROOTSYS_COMPLEX_REPLAY_INTERFACE_NAME`
  - `ROOTSYS_COMPLEX_REPLAY_INTERFACE_VERSION`

## 실행 방법

### 1) 프로파일 이름으로 실행
```bash
bash scripts/run_all_checks_and_prepare_ui.sh default
```

### 2) 설정 파일 직접 지정
```bash
ROOTSYS_CONFIG_FILE=/absolute/path/to/company.env bash scripts/run_all_checks_and_prepare_ui.sh
```

## 동작 원리
- 모든 실행 스크립트는 `scripts/lib/company_config.sh`를 통해 설정 파일을 로드합니다.
- 필수 파일(계약/인터페이스) 경로가 존재하지 않으면 실행 전에 즉시 실패합니다.
- 상대 경로는 프로젝트 루트 기준 절대 경로로 정규화되어, 어느 위치에서 실행해도 동일하게 동작합니다.
