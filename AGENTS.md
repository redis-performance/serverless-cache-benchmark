# Agent guidelines

Instructions for AI coding agents (Claude Code, Copilot, Cursor, etc.) working in this repo.

## Project overview

`serverless-cache-benchmark` is a Go CLI tool that benchmarks serverless cache services — primarily Redis-compatible endpoints and Momento — under realistic, configurable workloads. It drives concurrent client connections with Zipf-distributed key access patterns, configurable set/get ratios, and optional traffic-shaping via a CSV file (time_seconds, clients, qps). Results are written to CSV and optionally pushed to AWS CloudWatch. The tool is designed to run on cloud VMs to measure latency and throughput from the application side, making it useful for comparing serverless cache offerings under production-like conditions.

## Local setup

```bash
git clone git@github.com:redis-performance/serverless-cache-benchmark.git
cd serverless-cache-benchmark
go mod download
make build
```

This produces a `serverless-cache-benchmark` binary in the current directory. Go 1.24 or later is required.

Quick smoke-test against a local Redis instance:

```bash
./serverless-cache-benchmark run --cache-type redis --redis-uri redis://localhost:6379 --clients 4 --test-time 10
```

For Momento, export `MOMENTO_API_KEY` and pass `--cache-type momento --momento-cache-name <name>`.

## Branch naming

Same as human contributors: `<type>/<short-description>` (e.g. `fix/zipf-exponent-bounds`).

## Coding standards

- Match the style already in the file you are editing.
- Prefer clear, minimal changes over large refactors unless explicitly asked.
- Do not add comments that describe *what* the code does — only add comments when the *why* is non-obvious.
- Do not introduce new dependencies without checking with the maintainer.
- Run `make checkfmt` before committing; CI enforces `gofmt` formatting.

## Running tests

```bash
make test
```

This downloads dependencies, builds an instrumented binary, runs all Go tests with coverage enabled, and prints a coverage summary. Always run this before declaring a task complete.

## How to submit changes

1. Create a branch: `git checkout -b <type>/<description>`.
2. Commit with a clear message focused on *why*, not *what*.
3. Open a pull request against `main`.
4. Do **not** push directly to `main`.

## What to avoid

- Do not reformat files unrelated to your change.
- Do not remove error handling or tests.
- Do not commit secrets, credentials, or large binary files.
- Do not amend published commits.
