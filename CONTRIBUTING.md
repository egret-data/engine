# Contributing to Egret Engine

Thanks for your interest in contributing! This document describes how we work.

## Workflow

1. **Open an issue first** for any non-trivial change (feature, design change, bug). Discuss the approach there.
   Larger features need a design document under `docs/superpowers/specs/` reviewed in a pull request before
   implementation starts.
2. **Fork and branch.** Use a descriptive branch name with a prefix: `feat/`, `fix/`, `docs/`, `chore/`,
   `refactor/`, `test/`.
3. **Keep pull requests focused.** One logical change per PR. Reference the issue in the PR description with
   `Closes #<number>` (not in individual commit messages).
4. **Pass CI.** Formatting, lints and tests must pass.
5. **Review.** At least one maintainer approval is required. PRs are squash-merged.

## Commit messages

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```text
feat(iceberg): add snapshot diff API
fix(pgwire): handle empty query string
docs: describe cache affinity scheduling
```

Common types: `feat`, `fix`, `docs`, `refactor`, `test`, `perf`, `build`, `ci`, `chore`.

## Development

The Rust workspace is not scaffolded yet. Once it is, the standard checks will be:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Code guidelines

- Write tests for new features and bug fixes (Arrange / Act / Assert).
- Follow existing patterns in the codebase; prefer readability over cleverness.
- Fail fast with specific, contextual errors.
- Never commit secrets or credentials.
- Update documentation when behaviour changes.
- Label Markdown code fences with a language identifier.

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
