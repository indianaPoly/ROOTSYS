## 바이브코딩으로 날먹하기

slack + codex를 조합하여 말로만으로 더 좋은 코드를 만들기 위해서 시작한 template입니다.

### 전체 사용 흐름

1. 사용자는 slack (open ai 연동)을 활용하여 PR을 생성할 수 있습니다.
- slack에서 특정 repo에 접근을 하기 위해서는 웹사이트로 접근해 environment를 직접 설정을 해주어야 함.
- 추후에는 jira도 연동해서 task만 추가하면 알아서 PR 만들고 코드도 바로 작업할 수 있도록 고도화 진행 예정

2. codex가 repo의 AGENTS.md 규칙을 읽음

3. scope-guard로 범위를 통제한 상태에서 작업을 수행

4. PR을 하기 전에 pr-preflight 절차를 통해서 해당 코드가 실행 가능한지 검증을 진행

5. PR 초안 생성

### slack에서 잘 사용하는 방법

#### 작업을 요청하는 경우
```txt
[작업 요청]

목표:
- 현재 개발되어있는 로그인 페이지 UI를 react-hook-form을 활용해서 연결 진항
- input 조건은 util 함수를 통해서 관리를 진행하도록 함.
- util 함수에 대한 test 코드 작성

범위:
- frontend/src/components/auth/LoginForm.tsx
- 관련된 util 함수 폴더

비범위:
- 공통 컴포넌트

조건:
- eslint airbnbn 스타일 가이드 유지
- scope-guard 기준으로 garbage diff 방지

작업 후:
- pr-preflight 기준으로 검증
- build 성공 후 PR 초안 작성
```

#### 브랜치를 검증하는 경우
```txt
[검증 요청]

목표: 
- 특정 repo의 특정 브랜치가 PR 가능한 상태인지 검증
- pr-preflight 기준으로 base 최신화 여부 확인
- lint -> test -> build 순으로 검증
- 실패 원인 분석
- 성공시에는 PR 초안 작성
```

### 각 파일들이 하는 역할
1. SKILL.md: codex가 특정 상황에서 따라야할 업무 지침서

2. pr-preflight: PR을 올리기 전에 반드시 거쳐야하는 검증 절차
- base 브랜치(현재 브랜치) 최신화 (merge/rebase)
- lint -> test -> build
- 빌드 실패 시 수정 후 재시도
- 빌드 성공시 PR 초안 작성

3. scope-guard: 작업 범위를 벗어나는 것을 방지하는 안전장치

4. quality-guard: 안티패턴을 배제하고 코드 퀄리티를 올리기 위한 안전장치

5. frontend-quality: 프론트엔드(React/Next.js) 코드 퀄리티 패턴 + 예시 모음
