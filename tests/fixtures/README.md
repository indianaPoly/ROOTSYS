# Fixtures

## 목적
로컬에서 파이프라인의 실제 동작을 검증하기 위한 샘플 DB 및 인터페이스 정의를 제공합니다.

## 구성
- `tests/fixtures/db/mes.db`: MES 샘플 SQLite DB
- `tests/fixtures/db/qms.db`: QMS 샘플 SQLite DB
- `tests/fixtures/interfaces/mes.db.json`: MES DB 인터페이스 정의
- `tests/fixtures/interfaces/qms.db.json`: QMS DB 인터페이스 정의
- `tests/fixtures/interfaces/rest.sample.json`: REST 인터페이스 예시
- `tests/fixtures/interfaces/postgres.sample.json`: Postgres 인터페이스 예시
- `tests/fixtures/interfaces/mysql.sample.json`: MySQL 인터페이스 예시
- `tests/fixtures/interfaces/invalid/*.json`: JSON Schema negative-case 인터페이스 fixture
- `tests/fixtures/ontology/materialization.input.jsonl`: Ontology materialization fixture input
- `tests/fixtures/ontology/materialization.expected.jsonl`: Ontology materialization expected output

## 생성
```bash
python3 scripts/create_sample_dbs.py
```
