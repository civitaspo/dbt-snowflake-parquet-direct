# Agent instructions

Use English for all repository text and public repository communication, including issues, pull requests, reviews, and comments.

Before creating or changing GitHub Actions workflows, composite actions, CI scripts, or CI configuration, read and follow `docs/github-actions.md` and keep it current.

Run `mise run lint` and `mise run test`; use `mise run fix` for supported automatic fixes and review the resulting diff.

Every commit must include a `Co-Authored-By: Name <email>` trailer for each contributing AI agent. Use the agent's actual identity, such as `Co-Authored-By: Codex <noreply@openai.com>`. Verify trailers before pushing. Immediately before requesting a csm-ci squash merge, after automated PR-body updates, put the contributors' trailers in the final trailer block of the PR body, separated from earlier text by a blank line. Verify the merged commit with `git show -s --format=%B <merge-sha> | git interpret-trailers --parse`.
