# company profiles (한국어)

ROOTSYS 실행 스크립트에서 사용하는 고객사/기업별 프로파일 파일 디렉터리입니다.

## 파일
- `default.env`: 기본 로컬 프로파일
- `acme.sample.env`: 샘플 프로파일
- `first-customer.sample.env`: 첫 고객사 온보딩용 시작 템플릿

## 신규 프로파일 생성
```bash
bash scripts/create_company_profile.sh <company-name>
```

## 프로파일 검증
```bash
bash scripts/validate_company_profile.sh <company-name>
```

## 주요 데이터 스케일 설정
- `ROOTSYS_SMOKE_DB_COUNT`
- `ROOTSYS_SMOKE_REST_COUNT`
- `ROOTSYS_COMPLEX_STREAM_RECORD_COUNT`
- `ROOTSYS_COMPLEX_INTERVAL_RUNS`
- `ROOTSYS_COMPLEX_REPLAY_INPUT_COUNT`
