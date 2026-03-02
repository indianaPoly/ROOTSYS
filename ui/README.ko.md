# ROOTSYS UI (한국어)

Next.js 16 기반 런타임 아티팩트 가시화 대시보드입니다.

## 목적
- smoke/complex 실행 결과 아티팩트 상태 표시
- 아티팩트별 레코드 수, 유니크 ID, source, 샘플 ID 제공

## 실행
```bash
npm install
npm run dev
```

브라우저에서 `http://localhost:3000` 접속.

## 읽는 아티팩트 경로
- `/tmp/rootsys-smoke/*`
- `/tmp/rootsys-complex/*`

## 빌드 검증
```bash
npm run typecheck
npm run build
```
