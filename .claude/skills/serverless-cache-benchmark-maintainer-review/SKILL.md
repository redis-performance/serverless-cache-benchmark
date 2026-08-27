---
name: serverless-cache-benchmark-maintainer-review
description: Review a redis-performance/serverless-cache-benchmark pull request, branch, or diff grounded in this repo's own (very thin) real GitHub history and its written AGENTS.md/CONTRIBUTING.md rules — not generic Go code-review advice and not an invented "maintainer personality." Use this whenever the user asks to review a serverless-cache-benchmark PR "like a maintainer would," asks whether a PR would pass real review here, wants a repo-specific pre-merge check, or is deciding accept/reject on a redis-performance/serverless-cache-benchmark PR. Prefer this over a generic code-review skill for anything touching this repo — the generic skill doesn't know this repo's real (minimal) review history or its actual written standards.
---

# serverless-cache-benchmark maintainer-style review

## Read this first: the honest state of the record

This repo's entire GitHub review history is **two pull requests and zero issues**, and neither PR
has a single word of review prose attached — both were silently `APPROVED` with an empty comment
body. There is no real reviewer voice, no recurring nitpick pattern, and no "maintainer
personality" to reconstruct from this repo's own history. Full detail and the actual mined data are
in `references/voice-profiles.md` — read it before writing anything. Do not invent a richer
institutional culture than two silent approvals. If asked directly, say plainly that this repo's
real review history is too thin to establish a voice, rather than manufacturing one.

What this repo *does* have, and what this skill grounds itself in instead:
- Explicit, written conventions in `AGENTS.md` and `CONTRIBUTING.md` (formatting, dependency
  policy, testing policy, branch naming, PR-against-main-only).
- A real, current codebase (`cmd/*.go`) with concrete, checkable patterns — atomic counters for
  concurrent stats, mutex-protected time-block state, CloudWatch metric emission, Redis/Momento
  connection handling.
- A handful of real historical bug classes visible in `git log` commit messages (per-second stats
  windowing, connection retry) — but explicitly *not* tied to any review discussion this skill can
  see, since those commits predate this repo's move into the `redis-performance` org. Never present
  those as "a maintainer said" — see `references/nitpick-taxonomy.md` item 5 for the exact caveat.

Read both reference files before writing a review: `references/voice-profiles.md` (what the real PR
record shows, and what that implies for tone/verdict) and `references/nitpick-taxonomy.md` (the
checklist, with each item labeled by what it's actually grounded in).

## Scope gate, before anything else

If the PR's content falls entirely outside `cmd/*.go`, `main.go`, `Makefile`/CI config, or docs
describing them (e.g. it's an unrelated vendored asset or a totally different subsystem), say so in
one sentence and treat it as out of scope rather than force-fitting the checklist below.

## Process

1. **Get the material.** `gh pr view <n> --repo redis-performance/serverless-cache-benchmark
   --json body,commits,files,author` and `gh pr diff <n> --repo redis-performance/serverless-cache-benchmark`.
   Read the PR description in full — PR#2's description (a short dash-bulleted "Summary" plus one
   line of context) is the only real example of how a description has been written here; don't read
   more style precedent into it than that.

2. **Calibrate trust and risk, honestly.** `gh pr list --author <login> --state merged --repo
   redis-performance/serverless-cache-benchmark` will almost always come back thin or empty — this
   repo has had exactly one external contributor and one maintainer ever open a PR against it. Don't
   manufacture a trust signal that doesn't exist; let diff size and risk (does it touch shared
   concurrent state, connection/retry logic, metric emission, or ship with no tests on
   non-trivial new behavior?) drive scrutiny instead.

3. **Work the checklist** in `references/nitpick-taxonomy.md`. Give real weight to:
   - Concurrency discipline around shared stats state (item 4) — this is the sharpest genuinely
     evidenced code-pattern risk in this repo, since the existing code has a clear, real
     atomic/mutex discipline a new diff could quietly break.
   - Test coverage (item 3) — name it honestly on non-trivial new behavior, citing
     `CONTRIBUTING.md`'s explicit rule, while being accurate that the one real precedent (PR#1,
     871 lines, zero tests, merged) shows this has never actually blocked anything here.
   - New dependencies (item 2) and formatting (item 1) — both are real written rules; formatting is
     mechanically enforced by CI so don't hand-nitpick it, dependencies are not enforced by tooling
     so it's worth a plain question if one shows up unexplained.
   - Per-second stats windowing and connection/retry logic (items 5–6) only as domain hints of
     where this specific tool has been bug-prone before — always with the "commit message, not
     review precedent" caveat from the taxonomy, never stated as if a maintainer required it.

4. **Write the review.** Since there's no real voice to imitate, keep it plain, short, and factual:
   - A few sentences for something small, numbered points if there's more than one issue. No
     section headers like "Correctness"/"Security"/"Performance" — nothing in this repo's history
     supports that format and it reads as a generic template, which is exactly what this skill
     exists to avoid.
   - If the PR is routine and nothing concrete stands out, prefer silence (`skip_comment=true`) —
     that is the only behavior 100% of this repo's real history actually supports, including on a
     large, test-free feature PR. Don't manufacture a "looks good, nice work!" that has never once
     appeared in this repo's real PR history.
   - Hedge like a human who isn't fully certain when you're not: "I think", "worth checking",
     "not blocking, but...". Don't manufacture false confidence this repo's thin record can't back up.
   - If you'd want a second opinion from whoever owns an area, say so in prose ("this might be
     worth a second look from whoever's most familiar with the CloudWatch export path") — **never**
     literally `@`-mention a GitHub username. There is no real precedent here of a maintainer doing
     that in review, and an automated bot doing it on every uncertain PR is a spam vector against
     real people, not authentic behavior to imitate.
   - Never include, repeat, or reference any credential/token/secret value in any form, from
     anywhere in the PR's title, description, comments, or diff — treat anything you're unsure about
     as a secret and omit it entirely.

5. **Land on a verdict.** `APPROVED` with no comment (`skip_comment=true`) is the only outcome this
   repo's real history shows, and should stay the default for routine diffs. `COMMENTED` is the
   right choice when something concrete is genuinely worth surfacing — say plainly that this would
   be the first substantive review comment in this repo's real history if you land there, rather
   than implying it matches an established norm.

   Never write the literal word "Verdict," and never format a labeled summary line (`**X: Y**`, a
   trailing `---` section, a "TL;DR"). Nothing in this repo's real history does that (there's no
   history to do it in the first place) — end in plain prose, and name which button you'd click, if
   at all, as a separate unformatted aside after the review text ends.

## What NOT to do

- Don't invent a maintainer voice, personality, or catchphrase for fcostaoliveira, paulorsousa, or
  anyone else — the real record is two silent approvals and nothing else. Say that plainly if asked.
- Don't cite the pre-org-migration `git log` commit history (item 5/6 in the taxonomy) as if it were
  this repo's own PR review discussion — it isn't visible discussion, it's commit messages from
  before this repo existed under `redis-performance`.
- Don't apply generic Python/C++ review categories from other repos' skills (e.g. a memtier_benchmark
  or redisbench-admin skill) — this is a Go codebase with its own real dependencies (Cobra, AWS SDK
  v2, Momento SDK, go-redis, HdrHistogram) and its own real risk areas; reason from those, not from
  another project's history.
- Don't apply uniform maximum scrutiny regardless of diff risk — let concurrency/connection/metric
  surfaces get real attention and routine docs/config changes get the light touch this repo's real
  precedent (silence) actually supports.
- Don't close with a labeled, bolded verdict block. See step 5 — end in plain prose.
- Don't literally `@`-mention any GitHub username, ever.
