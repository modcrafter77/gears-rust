# CI cache benchmark: results and handoff

Paired benchmark of `codex/ci-optimization` against the original CI on the fork
`modcrafter77/gears-rust`, run with `tools/ci/benchmark.py` (see `tools/ci/README.md`
for the protocol). Measurements are job run time (start to end), never queue time.

## Outcome (exp8 vs baseline)

Warm runs, same commit as the cache (best case). Baseline: 3 warm runs, exp8: 6 warm runs.

| | baseline | exp8 warm | gain |
|---|---|---|---|
| Run wall time (first job start → last job end) | 85 min (81–105) | 72 min (64–72) | −15%, ranges do not overlap |
| Sum of job medians (runner minutes) | 377 | 311 | −17% |
| Test Suite (windows), critical path | 81 (64–105) | 72 (64–73) | −11% |
| Test Suite (ubuntu) | 38 (37–54) | 30 (24–32) | −20% |
| Test Suite (macos), no cache | 76 (70–85) | 56 (45–61) | −26% |
| Integration tests | 65 (56–65) | 54 (41–56) | −16% |
| FIPS (ubuntu) | 21 | 9 | −58% |
| Architecture Lints | 14 | 9 | −36% |

Warm sccache hit rate: Test Suite Linux 100%, FIPS Linux 88%, Test Suite Windows 84%,
Integration 67%, Clippy 38% (check-only). Archives in the cache: Linux 2.7 GB,
Windows 3.0 GB, plus a few 205 MB rust-cache entries (~7 GB of the 10 GB quota).

`benchmark.py compare` refuses the exp8 samples because cargo-deny fails in every run
(new `zip` advisories, unrelated to caching); the table excludes only cargo-deny and
Magika. Wall times of two exp8 runs where Windows tests failed early are excluded.

## What was found

1. **Original CI never persisted sccache.** `SCCACHE_GHA_ENABLED` was written to
   `$GITHUB_ENV` after `sccache --start-server`, so every job used a local, empty cache.
2. **Per-object GHA sccache does not work at this scale.** Every write error was
   `HTTP 429 rate limit exceeded` on `CreateCacheEntry` (repository-wide limit), and when
   the startup write check hit 429, sccache ran the whole job read-only.
3. **`SCCACHE_GHA_VERSION` alone enables the GHA backend** (sccache 0.18 config.rs), which
   silently gave the first baseline the fix it was measured against (fixed in baseline-exp3).
4. **The 10 GB quota is binding.** Per-job archives (exp6) were evicted minutes after
   saving; one archive per OS (exp7) fits.
5. **`SCCACHE_CACHE_SIZE=2G` was below the ~3 GiB per-OS working set** (measured with
   `ci-bench-measure-sccache`), so the LRU thrashed: 10% hits on identical code (exp7).

## Design in exp8 (`ci-bench-optimized-exp8`)

- sccache on local disk; one archive per OS restored with `actions/cache/restore`
  (prefix fallback) and saved once by the `test` job (`.github/actions/sccache*`).
- Writes only on push to `main` (and benchmark dispatches); pull requests read only,
  so they cannot evict the main cache. Same `save-if` on every rust-cache step.
- `SCCACHE_CACHE_SIZE: 3.5G`; no sccache archive for macOS (quota; not critical path).
- Stable-toolchain Linux jobs share one rust-cache key (targets off ⇒ same registry).

## Branches on the fork

| Branch | Purpose |
|---|---|
| `ci-bench-baseline-exp4` | Baseline: original CI + benchmark inputs, local sccache like upstream, ledger race fix |
| `ci-bench-optimized-exp5` | Diagnostics: sccache server log in artifacts (found the 429s) |
| `ci-bench-optimized-exp6` | Per-job local sccache archives (did not fit the quota) |
| `ci-bench-optimized-exp7` | One archive per OS, 2G cap (LRU thrash) |
| `ci-bench-optimized-exp8` | **Current design**, measured above |
| `ci-bench-measure-sccache` | Uncapped sccache, no cache saves: working-set measurement |
| exp2/exp3 branches | Superseded (Codecov/rustls fixes, baseline sccache leak) |

All bench branches carry the same `fix(ledger): replay a racing identical dispute outcome`
from exp4 on (flaky `concurrent_lost_on_one_dispute_claws_back_once`); that bug exists on
`main` and deserves its own PR.

## Results layout

- `expN/baseline.json`, `expN/optimized.json`: `benchmark.py collect` samples.
- `expN/orchestrate.log`: run IDs, failures and cache contents after each run.
- `exp7/compare.txt`: last formal `benchmark.py compare` output.
- `scripts/orchestrate*.py`: the series drivers (v6 = exp8); `clean_cache.sh` deletes
  fork caches with API rate-limit back-off; `jobs.sh` prints per-job timings.

## Next steps

1. Bump `zip` (three new advisories) in a separate PR to `main`; cargo-deny fails everywhere.
2. Measure the real PR scenario: a fork PR with a small change reading the main cache,
   against the same PR on the baseline. Same-commit warm runs above are the best case.
3. Port the exp8 design into `codex/ci-optimization` and attach these numbers to its PR.
4. Before a new series, clear the fork cache (`scripts/clean_cache.sh`): both variants'
   caches together exceed the 10 GB quota.
