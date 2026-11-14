# GitHub Actions architecture

The PR entry workflow runs on every pull request and calls the local reusable checks workflow. Its `status-check` gate runs even when checks fail or are cancelled and succeeds only when the called workflow succeeds. The checks run `mise run lint` and `mise run test` on the PR head. Securefix is restricted to trusted same-repository PRs and runs `mise run fix` separately so it cannot hide lint failures.

Start workflows with `permissions: {}` and grant each job only what it needs. Pin external actions and reusable workflows to full commit SHAs, disable checkout credential persistence, specify timeouts, and pass only named secrets. Treat PR contents and event fields as untrusted. Never execute PR code with elevated credentials or under `pull_request_target`.

The csm-ci server manages its caller workflows. Do not edit generated callers by hand; use server-managed distribution. Keep the required `status-check` name unique, and update this document when the architecture changes.
