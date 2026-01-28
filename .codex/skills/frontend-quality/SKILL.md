---
name: frontend-quality
description: Improve frontend code quality by enforcing senior patterns and excluding anti-patterns (React + Next.js App Router). Includes concrete examples.
---

## When to use
- Frontend 작업을 요청받았을 때 (React/Next.js)
- "퀄리티", "구조 개선", "리팩토링", "패턴 적용", "예시 포함"이 포함된 요청
- PR diff가 커지기 시작할 때(구조 재정렬/컴포넌트 분리/라우팅 설계 필요)

---

## Hard Blocks (must not ship)
- 타입 안전성 우회 금지: `as any`, `@ts-ignore`, `@ts-expect-error`
- `useEffect`로 derived state 만들지 않기(필터링/정렬/계산은 render에서 `useMemo`로)
- Context에 keystroke-level state 넣지 않기(폼 입력/슬라이더 등)
- “God Context” 만들지 않기(서로 무관한 concerns를 한 Context에 몰아넣지 않기)
- 구현 디테일 테스트 금지(className/id 기반 query, 내부 state 직접 접근, shallow rendering)

---

## Decision Rules (senior patterns)

### 1) Props depth → Provider/Context 전환 기준
- 기본은 props로 1–2 depth 전달
- 다음 중 하나면 Provider/Context 고려:
  - 3+ depth prop drilling이 발생하고 중간 컴포넌트가 relay 역할만 하는 경우
  - 동일 데이터가 여러 sibling에서 필요해서 상위로 state를 계속 끌어올리게 되는 경우
  - 컴포넌트 props가 “데이터 plumbing” 위주로 커져서 API가 읽기 어려워지는 경우

### 2) Provider 패턴을 적용할 때의 품질 기준
- Context 값은 “Concern 단위”로 쪼개기(Theme/Auth/FeatureFlags 등)
- Provider `value`가 object면 `useMemo`로 identity 안정화
- "자주 바뀌는 값"은 Context로 올리지 말고 로컬 state/전용 store로 분리

### 3) 컴포넌트 분리 vs hook 추출
- hook 추출: UI와 무관한 로직(데이터 fetch, validation, derivation)이 테스트 가능한 형태로 분리될 때
- 컴포넌트 분리: re-render boundary가 필요하거나 props가 복잡해질 때(특히 boolean prop hell)
- 프리머추어 추상화 금지: “rule of three”(유사 패턴 3번 등장 전에는 일반화 지양)

### 4) Next.js App Router: Dynamic route vs searchParams
- Dynamic segment(`/products/[id]`): 리소스 identity (SEO/공유 링크/metadata, generateStaticParams 고려)
- `searchParams`: view state (필터/정렬/페이지네이션) — 단, page에서 사용하면 dynamic rendering에 영향
- “URL이 바뀌면 다른 컨텐츠로 인지되어야 하는가?” → 예면 path segment
- “같은 컨텐츠를 다른 뷰로 보는가?” → 예면 searchParams

### 5) Layout boundary
- Layout은 `searchParams`를 받지 않음. query-driven UI는 page 또는 client widget으로 격리

---

## Examples

### Example 1: 3+ depth prop drilling → Provider
```tsx
import React, { createContext, useContext, useMemo, useState } from 'react';

type Theme = 'light' | 'dark';

type ThemeContextValue = {
  theme: Theme;
  setTheme: (next: Theme) => void;
};

const ThemeContext = createContext<ThemeContextValue | undefined>(undefined);

function useTheme() {
  const ctx = useContext(ThemeContext);
  if (!ctx) throw new Error('Missing ThemeProvider');
  return ctx;
}

function ThemeProvider({ children }: { children: React.ReactNode }) {
  const [theme, setTheme] = useState<Theme>('light');
  const value = useMemo(() => ({ theme, setTheme }), [theme]);
  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

function App() {
  return (
    <ThemeProvider>
      <Shell />
    </ThemeProvider>
  );
}

function Shell() {
  return <Sidebar />;
}

function Sidebar() {
  return <Settings />;
}

function Settings() {
  const { theme, setTheme } = useTheme();
  return (
    <button type="button" onClick={() => setTheme(theme === 'light' ? 'dark' : 'light')}>
      Theme: {theme}
    </button>
  );
}
```

### Example 2: Next.js App Router — identity vs view state
```txt
# identity
app/products/[id]/page.tsx

# view state
app/products/page.tsx  (searchParams: q/sort/page)
```

---

## Testing

## Core Philosophy

**The more your tests resemble the way your software is used, the more confidence they can give you.**  
— Testing Library Guiding Principle

---

## Rules (20)

### Query Selection & Accessibility
1. **Query Priority Order**: `getByRole` > `getByLabelText` > `getByPlaceholderText` > `getByText` > `getByTestId` (use as last resort)
2. **Accessibility-First**: If you cannot query an element by role or label, fix the accessibility issue in the component rather than using test IDs
3. **Use `screen` Object**: Always import and use `screen` from render instead of destructuring query functions for better test readability
4. **Prefer `userEvent` over `fireEvent`**: `userEvent` simulates realistic browser interactions; `fireEvent` only dispatches low-level DOM events

### Behavior vs Implementation
5. **Test Behavior, Not Implementation**: Focus on what users see and do, not internal state, private methods, or component structure
6. **Don't Test Internal State or Private Functions**: These are implementation details; test through the public interface (visible UI)
7. **Avoid Testing "Is Rendering"**: Tests that only assert a component renders provide no value; focus on interactions and outcomes
8. **Don't Test Component Lifecycle Methods**: These are implementation details that users don't interact with
9. **Don't Test Child Component Details**: Treat child components as black boxes; only test the parent's observable behavior

### Mocking Strategy
10. **Mock External APIs Only**: Use tools like MSW to mock network requests; avoid mocking your component tree, hooks, or context
11. **Avoid Over-Mocking**: Try to avoid mocks until completely necessary; integration tests with real dependencies are more valuable
12. **Don't Mock Redux Selectors or Hooks**: Mocking library imports is fragile and breaks on refactoring
13. **Prefer MSW Over `jest.spyOn(fetch)`**: MSW works at the network level and is more robust for API mocking

### Test Structure & Scope
14. **Prefer Integration Tests Over Unit Tests**: Integration tests mock as little as possible (typically only network requests) and verify multiple units work together
15. **Use `findBy*` for Async Elements**: `findBy*` queries automatically retry and should be the default for elements that appear asynchronously
16. **Use `waitFor` for Complex Async Logic**: When waiting for state changes or multiple conditions, use `waitFor` with explicit assertions
17. **Avoid Manual `act()` Wrappers**: RTL handles `act` automatically; if you need it, you're likely testing implementation details
18. **Test Custom Hooks with `renderHook`**: When testing hook logic directly, use `renderHook` from `@testing-library/react`

### Error & Edge Cases
19. **Test Error States Through User-Visible Output**: Don't assert error objects or state; verify that error messages are displayed to users
20. **Test Error Boundaries by Verifying Fallback UI**: Render a component that throws an error, then assert the fallback UI appears

---

## Examples (10)

### 1. Form Testing with Accessibility-First Queries

```typescript
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'

test('submit form with valid data', async () => {
  const user = userEvent.setup()
  render(<LoginForm />)

  // Query by label (accessibility-first)
  const emailInput = screen.getByLabelText('Email')
  const passwordInput = screen.getByLabelText('Password')
  const submitButton = screen.getByRole('button', { name: /log in/i })

  // Simulate realistic user typing
  await user.type(emailInput, 'user@example.com')
  await user.type(passwordInput, 'securepassword')
  await user.click(submitButton)

  // Assert user-visible outcome
  expect(screen.getByText('Welcome, user!')).toBeInTheDocument()
})

test('shows validation errors for invalid email', async () => {
  const user = userEvent.setup()
  render(<LoginForm />)

  const emailInput = screen.getByLabelText('Email')
  await user.type(emailInput, 'invalid-email')
  await user.tab() // Blur to trigger validation

  expect(screen.getByText('Please enter a valid email')).toBeInTheDocument()
  expect(emailInput).toBeInvalid()
})
```

**Links**:
- [ByLabelText Query Docs](https://testing-library.com/docs/queries/bylabeltext/)
- [Accessibility-First Testing Guide](https://www.seanelliott.au/blog/2024-12-27-catching-accessibility-issues-early-with-react-testing-library/)

---

### 2. API Error State Testing with MSW

```typescript
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { setupServer } from 'msw/node'
import { http, HttpResponse } from 'msw'

const server = setupServer(
  http.get('/api/users', () => {
    return HttpResponse.json({ users: [] })
  }),
  http.post('/api/users', () => {
    return HttpResponse.json(
      { message: 'Server error' },
      { status: 500 }
    )
  })
)

beforeAll(() => server.listen())
afterEach(() => server.resetHandlers())
afterAll(() => server.close())

test('displays error message on server failure', async () => {
  const user = userEvent.setup()
  render(<UserForm />)

  const nameInput = screen.getByLabelText('Name')
  await user.type(nameInput, 'John Doe')
  await user.click(screen.getByRole('button', { name: /submit/i }))

  // Wait for user-visible error state
  await waitFor(() => {
    expect(screen.getByRole('alert')).toBeInTheDocument()
  })

  expect(screen.getByRole('alert')).toHaveTextContent(
    'Oops, failed to create user. Please try again.'
  )
  expect(screen.getByRole('button', { name: /submit/i })).toBeDisabled()
})
```

**Links**:
- [RTL Example with MSW](https://testing-library.com/docs/react-testing-library/example-intro/)
- [MSW Mocking Responses](https://mswjs.io/docs/basics/mocking-responses/)
- [Seamless API Mocking Guide (2025)](https://leapcell.io/blog/seamless-api-mocking-in-tests-with-mock-service-worker)

---

### 3. Router Testing with MemoryRouter

```typescript
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Routes, Route } from 'react-router-dom'

test('navigates to protected route when authenticated', async () => {
  const user = userEvent.setup()
  
  render(
    <MemoryRouter initialEntries={['/login']}>
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route path="/dashboard" element={<DashboardPage />} />
      </Routes>
    </MemoryRouter>
  )

  // Login interaction
  await user.type(screen.getByLabelText('Email'), 'user@example.com')
  await user.type(screen.getByLabelText('Password'), 'password')
  await user.click(screen.getByRole('button', { name: /log in/i }))

  // Verify navigation to dashboard
  expect(screen.getByRole('heading', { name: /dashboard/i })).toBeInTheDocument()
})

test('redirects to login for protected routes', () => {
  render(
    <MemoryRouter initialEntries={['/protected']}>
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route
          path="/protected"
          element={
            <ProtectedRoute>
              <DashboardPage />
            </ProtectedRoute>
          }
        />
      </Routes>
    </MemoryRouter>
  )

  // Should redirect to login
  expect(screen.getByText('Please log in to access this page')).toBeInTheDocument()
})
```

**Links**:
- [Testing Library React Router Example](https://testing-library.com/docs/example-react-router/)
- [React Router Testing Guide (2025)](https://www.dhiwise.com/blog/design-converter/a-complete-guide-to-react-router-testing-with-react)
- [React Router Testing Docs](https://reactrouter.com/start/framework/testing)

---

### 4. Testing Error Boundaries

```typescript
import { render, screen } from '@testing-library/react'
import { ErrorBoundary } from './ErrorBoundary'

// Suppress console.error in tests
const originalError = console.error
beforeAll(() => {
  console.error = jest.fn()
})
afterAll(() => {
  console.error = originalError
})

const ThrowError = () => {
  throw new Error('Test error')
}

test('renders fallback UI when child component throws error', () => {
  render(
    <ErrorBoundary fallback={<div>Something went wrong</div>}>
      <ThrowError />
    </ErrorBoundary>
  )

  expect(screen.getByText('Something went wrong')).toBeInTheDocument()
})

test('renders children normally when no error occurs', () => {
  render(
    <ErrorBoundary fallback={<div>Error fallback</div>}>
      <div>Normal content</div>
    </ErrorBoundary>
  )

  expect(screen.getByText('Normal content')).toBeInTheDocument()
  expect(screen.queryByText('Error fallback')).not.toBeInTheDocument()
})
```

**Links**:
- [RTL Error Boundary Testing](https://testing-library.com/docs/react-testing-library/faq/)
- [Testing Error Boundaries with RTL](https://jshakespeare.com/react-error-boundary-testing-rtl/)

---

### 5. Contract Testing with OpenAPI/TypeScript

```typescript
// Generate API client from OpenAPI spec in CI
// Type-check frontend against contract

import { api } from './generated/api-client'
import { render, screen, waitFor } from '@testing-library/react'
import { setupServer } from 'msw/node'
import { http, HttpResponse } from 'msw'

const server = setupServer(
  http.get('/api/users/:id', ({ params }) => {
    return HttpResponse.json({
      id: params.id,
      name: 'John Doe',
      email: 'john@example.com',
      // TypeScript will error if response doesn't match OpenAPI schema
    })
  })
)

beforeAll(() => server.listen())
afterAll(() => server.close())

test('API contract - fetch user returns expected shape', async () => {
  render(<UserProfile userId="123" />)

  await waitFor(() => {
    expect(screen.getByText('John Doe')).toBeInTheDocument()
  })

  // TypeScript ensures we only access properties defined in contract
  const emailElement = screen.getByText('john@example.com')
  expect(emailElement).toBeInTheDocument()
})
```

**Links**:
- [Contract Tests with TypeScript and OpenAPI Codegen (2025)](https://dev.to/tsirlucas/contract-tests-with-typescript-and-openapi-codegen-4o7g)
- [Pact Contract Testing Docs](https://docs.pact.io)
- [12 Contract-Testing Patterns (2025)](https://medium.com/@Modexa/12-contract-testing-patterns-that-keep-uis-moving-a2d9ebb49d8f)

---

### 6. Accessibility-First Testing with getByRole

```typescript
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'

test('search form is accessible and functional', async () => {
  const user = userEvent.setup()
  render(<SearchForm />)

  // Query by role (most preferred query)
  const searchInput = screen.getByRole('searchbox', {
    name: /search products/i
  })
  const searchButton = screen.getByRole('button', { name: /search/i })

  // Verify accessibility attributes
  expect(searchInput).toHaveAccessibleName()
  expect(searchButton).not.toBeDisabled()

  // Test interaction
  await user.type(searchInput, 'laptop')
  await user.click(searchButton)

  // Verify results
  expect(screen.getByRole('status', { name: /search results/i })).toBeInTheDocument()
})

test('modal dialog is accessible', () => {
  render(<ConfirmDialog />)

  const dialog = screen.getByRole('dialog', { name: /confirm action/i })
  const confirmButton = screen.getByRole('button', { name: /confirm/i })
  const cancelButton = screen.getByRole('button', { name: /cancel/i })

  expect(dialog).toBeInTheDocument()
  expect(confirmButton).toHaveFocus() // Initial focus
})
```

**Links**:
- [ByRole Query Docs](https://testing-library.com/docs/queries/byrole/)
- [Query Priority Guide](https://testing-library.com/docs/queries/about/)
- [Accessibility-First Integration Tests](https://www.ronaldjamesgroup.com/blog/accessibility-first-integration-tests-with-react-testing-library-by-suzanne-aitchison)

---

### 7. Async Testing with findBy and waitFor

```typescript
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'

test('loads data and displays after API call', async () => {
  render(<UserProfile userId="123" />)

  // Loading state
  expect(screen.getByRole('progressbar')).toBeInTheDocument()

  // Wait for data to load (findBy automatically retries)
  const userName = await screen.findByRole('heading', { name: /john doe/i }, {
    timeout: 3000
  })
  
  expect(userName).toBeInTheDocument()
  expect(screen.getByRole('progressbar')).not.toBeInTheDocument()
})

test('waits for multiple conditions before asserting', async () => {
  render(<Dashboard />)
  
  await waitFor(() => {
    // Multiple assertions in one waitFor
    expect(screen.getByRole('heading', { name: /dashboard/i })).toBeInTheDocument()
    expect(screen.getByRole('img', { name: /user avatar/i })).toBeInTheDocument()
    expect(screen.getByText('Welcome back!')).toBeInTheDocument()
  })
})

test('element is removed after action', async () => {
  const user = userEvent.setup()
  render(<NotificationBanner />)

  const banner = screen.getByRole('alert')
  await user.click(screen.getByRole('button', { name: /dismiss/i }))

  // Wait for element to be removed
  await waitForElementToBeRemoved(() =>
    screen.queryByRole('alert')
  )
  
  expect(screen.queryByRole('alert')).not.toBeInTheDocument()
})
```

**Links**:
- [Using Async Queries](https://testing-library.com/docs/dom-testing-library/api-async/)
- [Async Testing Best Practices](https://www.testingjavascript.com/lessons/react-test-drive-error-state-with-react-testing-library)

---

### 8. Custom Hook Testing with renderHook

```typescript
import { renderHook, act, waitFor } from '@testing-library/react'
import { useCounter } from './useCounter'

test('increments and decrements counter', () => {
  const { result } = renderHook(() => useCounter())

  expect(result.current.count).toBe(0)

  act(() => {
    result.current.increment()
  })

  expect(result.current.count).toBe(1)

  act(() => {
    result.current.decrement()
  })

  expect(result.current.count).toBe(0)
})

test('async hook with waitFor', async () => {
  const { result } = renderHook(() => useAsyncData())

  expect(result.current.data).toBeNull()
  expect(result.current.loading).toBe(true)

  await waitFor(() => {
    expect(result.current.data).not.toBeNull()
  })

  expect(result.current.loading).toBe(false)
})

test('updates hook when props change', () => {
  const { result, rerender } = renderHook(
    ({ initialValue }) => useCounter(initialValue),
    { initialProps: { initialValue: 5 } }
  )

  expect(result.current.count).toBe(5)

  rerender({ initialValue: 10 })

  expect(result.current.count).toBe(10)
})
```

**Links**:
- [React Hooks Testing Library API](https://react-hooks-testing-library.com/reference/api/)
- [Testing Hooks with renderHook (2025)](https://javascript.plainenglish.io/test-react-hooks-the-practical-way-three-patterns-that-always-hold-up-2025-3429319daef2)

---

### 9. userEvent vs fireEvent: Realistic Interaction

```typescript
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { fireEvent } from '@testing-library/react'

// BAD: fireEvent only dispatches low-level DOM events
test('bad: using fireEvent', () => {
  render(<LoginForm />)
  
  fireEvent.change(screen.getByLabelText('Email'), {
    target: { value: 'user@example.com' }
  })
  fireEvent.click(screen.getByRole('button', { name: /submit/i }))
  
  // May not trigger all real user events (focus, blur, etc.)
})

// GOOD: userEvent simulates realistic browser behavior
test('good: using userEvent', async () => {
  const user = userEvent.setup()
  render(<LoginForm />)
  
  // userEvent.type() fires keydown, keypress, keyup, input, change events
  await user.type(screen.getByLabelText('Email'), 'user@example.com')
  await user.type(screen.getByLabelText('Password'), 'password123')
  
  // userEvent.click() checks visibility and interactability
  await user.click(screen.getByRole('button', { name: /submit/i }))
  
  // More realistic, includes focus management and event sequence
})

test('userEvent handles complex interactions', async () => {
  const user = userEvent.setup()
  render(<Autocomplete />)
  
  // Tab, type, select from dropdown
  await user.tab()
  await user.keyboard('app')
  await user.keyboard('{ArrowDown}{Enter}')
  
  expect(screen.getByText('Apple')).toBeInTheDocument()
})
```

**Links**:
- [userEvent Intro Docs](https://testing-library.com/docs/user-event/intro/)
- [fireEvent Considerations](https://testing-library.com/docs/guide-events/)
- [userEvent Utility APIs](https://testing-library.com/docs/user-event/utility)

---

### 10. Integration Test: Full User Flow

```typescript
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { setupServer } from 'msw/node'
import { http, HttpResponse } from 'msw'
import { MemoryRouter, Routes, Route } from 'react-router-dom'

const server = setupServer(
  http.post('/api/auth/login', () => {
    return HttpResponse.json({ token: 'fake-jwt-token' })
  }),
  http.get('/api/user/profile', () => {
    return HttpResponse.json({
      id: '1',
      name: 'John Doe',
      email: 'john@example.com'
    })
  })
)

beforeAll(() => server.listen())
afterEach(() => server.resetHandlers())
afterAll(() => server.close())

test('complete user flow: login -> dashboard -> logout', async () => {
  const user = userEvent.setup()
  
  render(
    <MemoryRouter initialEntries={['/login']}>
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route path="/dashboard" element={<DashboardPage />} />
        <Route path="/" element={<HomePage />} />
      </Routes>
    </MemoryRouter>
  )

  // Step 1: Login
  await user.type(screen.getByLabelText('Email'), 'john@example.com')
  await user.type(screen.getByLabelText('Password'), 'password123')
  await user.click(screen.getByRole('button', { name: /log in/i }))

  // Step 2: Verify navigation to dashboard
  await waitFor(() => {
    expect(screen.getByRole('heading', { name: /dashboard/i })).toBeInTheDocument()
  })

  // Step 3: Verify user profile loaded
  expect(screen.getByText('John Doe')).toBeInTheDocument()
  expect(screen.getByText('john@example.com')).toBeInTheDocument()

  // Step 4: Logout
  await user.click(screen.getByRole('button', { name: /log out/i }))

  // Step 5: Verify redirect to home
  await waitFor(() => {
    expect(screen.getByRole('heading', { name: /welcome/i })).toBeInTheDocument()
  })
  expect(screen.queryByRole('heading', { name: /dashboard/i })).not.toBeInTheDocument()
})
```

**Links**:
- [Static vs Unit vs Integration vs E2E Tests](https://kentcdodds.com/blog/static-vs-unit-vs-integration-vs-e2e-tests)
- [Integration Testing with Redux and RTL](https://redux.js.org/usage/writing-tests)
- [User-Centric Testing (2023)](https://marmelab.com/blog/2023/05/26/react-user-centric-testing.html)

---

## Additional Resources

### Official Documentation
- [Testing Library Guiding Principles](https://testing-library.com/docs/guiding-principles/)
- [React Testing Library Docs](https://testing-library.com/docs/react-testing-library/intro/)
- [Testing Library FAQ](https://testing-library.com/docs/react-testing-library/faq/)
- [Query Priority Guide](https://testing-library.com/docs/queries/about/)

### Articles & Guides
- [Testing Implementation Details - Kent C. Dodds](https://kentcdodds.com/blog/testing-implementation-details)
- [How to Know What to Test - Kent C. Dodds](https://kentcdodds.com/blog/how-to-know-what-to-test)
- [Avoid the Test User - Kent C. Dodds](https://kentcdodds.com/blog/avoid-the-test-user)
- [React Testing in 2025: Stop Mocking, Start Trusting](https://dev.to/tahamjp/react-testing-in-2025-stop-mocking-start-trusting-your-components-3h4f)
- [React Component Testing: Best Practices for 2025](https://dev.to/tahamjp/react-component-testing-best-practices-for-2025-2674)
- [A Guide To User Behavior Testing using RTL](https://www.f22labs.com/blogs/a-guide-to-user-behavior-testing-using-rtl-react-testing-library/)
- [Best Practices for React UI Testing in 2026](https://trio.dev/best-practices-for-react-ui-testing/)

### Contract Testing
- [Pact Documentation](https://docs.pact.io)
- [Contract Tests with TypeScript and OpenAPI Codegen (2025)](https://dev.to/tsirlucas/contract-tests-with-typescript-and-openapi-codegen-4o7g)
- [Entente - Modern Contract Testing Platform](https://docs.entente.dev/)

### MSW (Mock Service Worker)
- [MSW Documentation](https://mswjs.io)
- [MSW Browser Integration](https://mswjs.io/docs/integrations/browser/)
- [Seamless API Mocking with MSW (2025)](https://leapcell.io/blog/seamless-api-mocking-in-tests-with-mock-service-worker)
- [React API Mocking Tests (2025 Guide)](https://w3office.com/docs/react-tutorial/react-testing-and-debugging-guide/react-api-mocking-tests)

### React Router Testing
- [React Router Testing Guide (2025)](https://www.dhiwise.com/blog/design-converter/a-complete-guide-to-react-router-testing-with-react)
- [Testing Custom React Routers (2025)](https://blog.zenika.com/2025/06/18/a-practical-guide-to-testing-custom-react-routers)
- [React Router Testing Docs](https://reactrouter.com/start/framework/testing)

### Community Examples
- [PatternFly React Testing Wiki](https://github.com/patternfly/patternfly-react/wiki/React-Testing-Library-Basics,-Best-Practices,-and-Guidelines)
- [RTL + Vitest + MSW Examples](https://github.com/ryokryok/rtl-vitest-msw-examples)

---

## Anti-Patterns to Avoid

### ❌ DON'T: Test Internal State
```typescript
test('bad: tests internal state', () => {
  render(<Counter />)
  // Can't access state directly - implementation detail
  // This is what people try to do with Enzyme
})
```

### ✅ DO: Test Visible Output
```typescript
test('good: tests visible output', async () => {
  const user = userEvent.setup()
  render(<Counter />)
  
  await user.click(screen.getByRole('button', { name: '+' }))
  expect(screen.getByText('Count: 1')).toBeInTheDocument()
})
```

### ❌ DON'T: Test Private Methods
```typescript
test('bad: tests private method', () => {
  const wrapper = shallow(<MyComponent />)
  // wrapper.instance().handleClick() - can't do this in RTL
})
```

### ✅ DO: Test Through Public Interface
```typescript
test('good: tests through interactions', async () => {
  const user = userEvent.setup()
  render(<MyComponent />)
  
  await user.click(screen.getByRole('button', { name: 'Click me' }))
  // Assert the visible result of the internal method call
  expect(screen.getByText('Success!')).toBeInTheDocument()
})
```

### ❌ DON'T: Use getByTestId as Default
```typescript
test('bad: uses test ID everywhere', () => {
  render(<Button data-testid="submit-btn" />)
  expect(screen.getByTestId('submit-btn')).toBeInTheDocument()
})
```

### ✅ DO: Use Semantic Queries
```typescript
test('good: uses semantic query', () => {
  render(<Button>Submit</Button>)
  expect(screen.getByRole('button', { name: 'Submit' })).toBeInTheDocument()
})
```

### ❌ DON'T: Mock Everything
```typescript
test('bad: mocks internal dependencies', () => {
  jest.mock('./useAuth') // Don't mock your own hooks
  render(<Dashboard />)
})
```

### ✅ DO: Mock Only External APIs
```typescript
test('good: mocks only network requests', () => {
  server.use(
    http.get('/api/data', () => HttpResponse.json({ data: [] }))
  )
  render(<Dashboard />)
})
```

---

## Summary Checklist

Before committing a test, verify:

- [ ] Uses `screen` object instead of destructuring queries
- [ ] Queries use priority order (role > label > placeholder > text > test ID)
- [ ] Tests user-visible behavior, not implementation details
- [ ] Uses `userEvent` instead of `fireEvent`
- [ ] Avoids testing internal state or private methods
- [ ] Mocks only external APIs (using MSW), not internal components/hooks
- [ ] Uses `findBy*` for async elements instead of manual waiting
- [ ] Handles error states through user-visible output
- [ ] Includes assertions that verify the user's goal was accomplished
- [ ] Doesn't test "is rendering" without interaction
- [ ] Tests accessibility through semantic queries
- [ ] Integration tests mock minimally, unit tests for pure functions

**Tests that break when you refactor code without changing behavior are implementation-detail tests. Tests that survive refactoring are behavior tests.**
