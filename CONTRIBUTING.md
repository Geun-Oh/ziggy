# Contributing to ziggy

Thanks for your interest in contributing.

## Development setup

- Install Zig `0.15.2` or newer.
- Clone the repository.
- Run:

```bash
zig build test
```

## Workflow

1. Create a feature branch from `main`.
2. Keep changes small and focused.
3. Add or update tests for behavior changes.
4. Update documentation for user-visible changes.
5. Open a pull request with a clear description.

## Code guidelines

- Follow existing project style and naming patterns.
- Keep functions reasonably small and composable.
- Prefer explicit error handling.
- Avoid unrelated refactors in feature/fix PRs.

## Commit messages

Use Conventional Commits when possible:

- `feat:` new feature
- `fix:` bug fix
- `docs:` documentation changes
- `test:` test changes
- `refactor:` internal refactor without behavior change
- `chore:` maintenance work

Examples:

- `feat: add manifest fallback loading path`
- `fix: reject malformed varint decode in prefix block`

## Pull request checklist

- [ ] `zig build test` passes
- [ ] New behavior has tests (positive and/or negative as needed)
- [ ] Docs are updated
- [ ] No unrelated files changed

## Reporting issues

When opening an issue, please include:

- Zig version
- OS / architecture
- Reproduction steps
- Expected behavior
- Actual behavior
- Relevant logs or failing test output

## Security

Please do not disclose security vulnerabilities publicly first.
Open a private report to the maintainers if possible.
