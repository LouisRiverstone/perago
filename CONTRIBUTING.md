# Contributing to perago

Thanks for helping. The full guide is at **https://louisriverstone.github.io/perago/contributing.html** (source: [docs/contributing.md](docs/contributing.md)).

## The short version

```
git clone https://github.com/<you>/perago.git && cd perago
git checkout -b feat/my-change

gofmt -l .                  # must print nothing
for m in . redis sqs dynamodb beanstalkd migro internal/dbtest; do
  (cd $m && go vet ./... && go test -race -count=1 ./...)
done
```

The repository is several Go modules: a dependency-free core at the root, and one module per backend or integration (`redis`, `sqs`, `dynamodb`, `beanstalkd`, `migro`), plus `internal/dbtest` for the database tests.

Optional: `pre-commit install` runs `gofmt` and `golangci-lint` before each commit.

1. For anything bigger than a small fix, **open an issue first**.
2. Branch from `main`; keep the PR to one concern.
3. Write tests that fail without your change. A new driver must pass `internal/drivertest`, a new cache `internal/cachetest`.
4. Update the docs in `docs/` when user-visible behavior changes.
5. Use Conventional Commit messages (`feat:`, `fix:`, `test:`, `docs:`, `refactor:`, `chore:`).

Security problems: do not open a public issue. Use the **Security** tab → *Report a vulnerability*.

By contributing you agree your work is released under the [MIT license](LICENSE).
