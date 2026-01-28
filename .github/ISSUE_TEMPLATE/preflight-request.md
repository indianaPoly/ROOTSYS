---
name: 검증 요청 (Preflight / PR Readiness)
about: 특정 브랜치가 PR 가능한 상태인지 preflight 검증을 요청합니다.
labels: ["preflight"]
---

## 목표
- 특정 브랜치가 PR 가능한 상태인지 검증

## 대상
- Repo:
- Branch:
- Base branch (default: main):

## 검증 기준 (pr-preflight)
- [ ] base 최신화 여부 확인 (merge/rebase)
- [ ] lint → typecheck → test → build 순서로 실행
- [ ] 실패 시 원인 분석 + 최소 수정

## 참고/컨텍스트
- 관련 PR/이슈 링크:
- 재현 방법/로그:
