# Reviewer voice — what the real record actually shows

**Read this before writing anything in a maintainer voice.** The honest finding, mined directly
from this repo's GitHub history (`gh pr list --repo redis-performance/serverless-cache-benchmark
--state all`, `gh pr view <n> --json body,comments,reviews`), as of 2026-08-27:

- This repo has exactly **two** pull requests in its entire history. It has **zero** issues, ever.
- **Neither PR has a single word of review prose attached to it.** Both were closed with a bare
  GitHub "Approve" review whose comment body is the empty string. No one has ever left an inline
  comment, a `COMMENTED` review, a `REQUEST_CHANGES` review, or so much as a one-word "LGTM" on a
  PR in this repository.

The two data points in full:

| PR | Author | Size | PR description | Review | Reviewer |
|----|--------|------|-----------------|--------|----------|
| #1 "feat: contribute momento perf test changes" | eaddingtonwhite (external contributor) | +871/-219, 7 files, touches `cmd/momento.go`, `cmd/populate.go`, `cmd/run.go`, `cmd/stats.go`, `go.mod`/`go.sum`, `run.sh` | empty body | `APPROVED`, empty body | fcostaoliveira (MEMBER) |
| #2 "Add CONTRIBUTING.md and AGENTS.md" | fcostaoliveira (MEMBER) | +124/-0, 2 files | has a real, structured description (a dash-bulleted "Summary" plus one sentence of org-initiative context) | `APPROVED`, empty body | paulorsousa (MEMBER) |

That's the entire evidence base. Three people have ever touched this repo's PR history at all:
**fcostaoliveira** (git log shows he authored nearly the whole codebase and is the repo's clear
primary maintainer, though almost none of that authorship went through this repo's own PR process
— see the note below), **paulorsousa** (a MEMBER, appears exactly once, as a silent approver), and
**eaddingtonwhite** (an external contributor, appears exactly once, as a silent author).

A note on the older commits: `git log` shows many earlier commits whose messages reference PR
numbers like `(#3)`, `(#7)`, `(#11)` from authors such as eaddingtonwhite, danielamiao, and Anita
Ruangrotsakun. Those numbers do **not** correspond to any PR object in this repo via the GitHub
API — the repo's `pushed_at`/commit dates show this history was imported wholesale when the tool
was brought into the `redis-performance` org on 2025-09-01, and those PR numbers belong to a
different, earlier repository whose review threads are not visible from here. Do not cite them as
if they were this repo's own PR discussions — there is no review text behind them that this skill
can see or quote.

## What this means for an automated review

There is no institutional "voice" to imitate here — not a terse-fcostaoliveira pattern, not a
detailed-kei-nan pattern, nothing. Inventing one (a personality, a catchphrase, a "the maintainer
always asks about X") would be fabrication, not grounding. So:

- **Default hard toward `skip_comment=true`.** The only behavior this repo's real history actually
  supports, 2 times out of 2, is silent approval with no written comment — including on PR#1, a
  genuinely large (871-line) feature diff with zero tests added (see
  `nitpick-taxonomy.md` item 3). If a PR looks routine or self-evidently fine, matching this
  repo's real precedent means saying nothing, not manufacturing a "LGTM, nice work!" that has never
  actually appeared here.
- **When something concrete and substantive is genuinely wrong or worth flagging**, write a short,
  plain, factual comment — a few sentences, numbered if there's more than one point — grounded in
  the specific code path and in the written rules in `AGENTS.md`/`CONTRIBUTING.md` (the only real,
  citable "institutional standard" this repo has, since no reviewer prose exists to draw from).
  Don't try to sound like a particular person; there's no person's real review voice on record to
  sound like.
- **PR#2's own description** (fcostaoliveira, as *author*, not as reviewer) is the only piece of
  real prose associated with either PR: a short dash-bulleted "Summary" section plus one line of
  context tying the change to a broader org initiative. That's a reasonable structural note for
  how this repo's own PR descriptions are written, but it is not review commentary — don't present
  it as if a maintainer had reviewed anything in that style.
- Never claim a verdict pattern this repo hasn't shown. `APPROVED` with no comment is the only
  outcome on record. If you land on `COMMENTED` because something is genuinely worth surfacing,
  say so plainly rather than implying it matches some established norm — it would be the first of
  its kind here.
