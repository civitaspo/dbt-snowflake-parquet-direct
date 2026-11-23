# GitHub Actions architecture

The `CI` workflow in `pull_request.yml` calls `workflow_call_pr.yml`. Its required `status-check` job runs even when checks fail or are cancelled. It succeeds only if the called workflow succeeds.

The `Push` workflow runs on pushes to `main` and on manual dispatch. It calls `workflow_call_push.yml` and reports the result through `push-status-check`.

Both callers run `wc-lint.yml` and `wc-tests.yml`, which invoke `mise run lint` and `mise run test`. The PR caller also runs `wc-autofix.yml` for same-repository PRs authored by `civitaspo`. That workflow checks the PR head for `CHANGELOG.md` edits outside `release/next`, runs `mise run fix`, and sends any resulting changes to Securefix. Lint runs independently of autofix.

Start workflows with `permissions: {}` and grant each job only what it needs. Pin external actions and reusable workflows to full commit SHAs, disable checkout credential persistence, specify timeouts, and pass only named secrets. Treat PR contents and event fields as untrusted. Never execute PR code with elevated credentials or under `pull_request_target`.

Regenerate `.github/workflows/actions.lock` with `gh actions-lock --no-narrow` after changing workflow dependencies. Keep the full SHA pins in the workflow files.

The csm-ci server manages its caller workflows. Do not edit generated callers by hand; use server-managed distribution. Keep the required `status-check` name unique, and update this document when the architecture changes.
