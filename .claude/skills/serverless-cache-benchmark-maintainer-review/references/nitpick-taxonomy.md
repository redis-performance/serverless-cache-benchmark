# Review checklist — grounded in written rules and real code, not real review comments

This repo has no recurring-nitpick history to mine (see `voice-profiles.md`: two PRs, zero review
comments, ever). So instead of a taxonomy distilled from real reviewer complaints, this is a
checklist grounded in two things that genuinely exist: (1) this repo's own written rules in
`AGENTS.md` and `CONTRIBUTING.md`, and (2) concrete, current patterns in the `cmd/` source tree
that a change could plausibly break. Every item below says which of those two it's grounded in.
Don't treat any of this as "what the maintainers have complained about" — say so if asked, because
it isn't true.

## 1. Formatting — written rule, CI-enforced

`AGENTS.md`/`CONTRIBUTING.md`: "Run `make checkfmt` before pushing/committing; CI enforces `gofmt`
formatting." `make checkfmt` runs `gofmt -d .` and fails the build on any diff. Don't hand-nitpick
whitespace, import ordering, or brace style — tooling already gates this. Only mention formatting
if something has plainly slipped past `gofmt` (rare, since it's mechanical).

## 2. New dependencies — written rule, unverified in practice

`AGENTS.md`: "Do not introduce new dependencies without checking with the maintainer." This is a
real, explicit written rule. If a diff adds or changes an entry in `go.mod`/`go.sum` beyond what's
needed for the stated change, name it and ask (in prose, not as an accusation) whether it was
cleared with a maintainer — don't assume it wasn't, since there's no real precedent either way to
say how strictly this has ever actually been enforced (the two real PRs on record didn't add a new
top-level dependency).

## 3. Test coverage — written rule, contradicted by the one real precedent that exists

`CONTRIBUTING.md` states, in writing: "All new behaviour must be covered by tests" and "Coverage
should not decrease." Be accurate about what the record actually shows: **this repository contains
zero `_test.go` files anywhere**, including after PR#1 — a 871-line, 7-file feature addition
(Momento perf-test support) — which added no tests at all and was still merged, same-cycle, with a
contentless `APPROVED` review. So: it is fair and worth naming when a non-trivial diff adds new
logic with no test coverage, citing the written rule — but do **not** claim it will block merge,
since the one real data point available shows the opposite happening in practice. State both halves
honestly if you raise it at all.

## 4. Concurrency correctness around shared stats state — real, evidenced code pattern

The codebase's existing concurrency discipline is concrete and checkable:
- `cmd/stats.go` and `cmd/populate.go` update shared counters (`TotalOps`, `SuccessOps`,
  `FailedOps`, `ActiveConns`) exclusively via `atomic.AddInt64`/`atomic.LoadInt64`, from worker
  goroutines coordinated with a `sync.WaitGroup`.
- `cmd/run.go` has a `mutex sync.Mutex` and a `BlockMutex sync.RWMutex` explicitly documented in
  the source as protecting "time block operations."

If a diff adds a new piece of state that's written from more than one goroutine (a new counter, a
new per-block field, anything read by the live progress/reporting path while workers are still
running), check that it follows the same atomic-or-mutex discipline already established next to it,
rather than a plain unsynchronized field. This is a concrete regression class in a concurrent
load-generator, not a generic "Go concurrency" lecture — point at the specific existing pattern
(atomic counter vs. `BlockMutex`) the new code sits next to.

## 5. Per-second stats window correctness — historical bug class, but *not* a review precedent

Caveat up front: this is grounded in raw `git log` commit messages, not in any PR review thread
this skill can see — those commits (`a379a08`, `b347ecf`, and others referencing PR numbers like
`#4`/`#7`) predate this repo's move into the `redis-performance` org and have no visible discussion
attached here. Cite them as "this exact class of bug has happened in this codebase before," never
as "a maintainer flagged this in review" — no one did, on the record available.

With that caveat: the commit history shows this area was genuinely fiddly — one fix for "progress
bar shows current second stats not cumulative," a follow-up "try returning previous second data to
avoid undercounting." If a diff touches the per-second stats window, the live progress bar, or
CSV/CloudWatch export of interval stats, it's worth actually tracing whether a value is being read
before or after the window it's meant to represent has closed, since that's the specific way this
tool has gotten it wrong before.

## 6. Connection lifecycle / retry logic — real feature-history area, same caveat as #5

Flags like `--connection-delay-ms`, a reconnect interval, and connection timeouts exist because
this tool drives many concurrent client connections against Redis and Momento endpoints. Commit
history (same pre-org caveat as item 5) includes a fix titled "retry momento client creation in
case of network issues." If a diff touches connection setup, retry, or timeout logic, reason
concretely about what happens under a slow or failing endpoint at the configured concurrency —
don't just check that the happy path compiles.

## 7. CloudWatch metric emission — real, current, actively-iterated surface

`aws-sdk-go-v2/service/cloudwatch` is a real, current dependency, and commit history shows several
follow-on iterations on it (making the metric window configurable, adding a group tag for
aggregation, adding TCP-connection-count as a metric). If a diff touches metric emission, sanity
check the dimensions, units, and aggregation window make sense — this area has visibly needed more
than one pass to get right, which is a reasonable, evidenced signal to look closely rather than
assume it's simple.

## Scope gate

If a diff touches none of `cmd/*.go`, `main.go`, the `Makefile`/CI config, or `run.sh`/docs that
describe them, say so in one sentence and treat it as out of scope rather than force-fitting the
items above onto it.

## What this taxonomy is honestly silent on

- Any real reviewer voice or personality (see `voice-profiles.md` — none exists).
- What a `REQUEST_CHANGES` review, or any substantive review comment at all, actually looks like in
  this repo — it has never happened.
- Whether the "no new dependencies without asking" or "tests required" rules are actually enforced
  under pressure — the sample size for both is effectively zero-to-one real PR.
- Anything about the Momento SDK's or go-redis client's own correctness — this taxonomy only covers
  this repo's own code and process, not its dependencies' internals.
