# CI benchmark baseline

Source base: `ebdb691f9a08fd1ff442d53abf77e49b82c53143`.
Compare with the frozen local `ci-bench-optimized` branch.

This experiment branch retains the original Makefile, hidden tool installations,
local sccache startup order, and original test commands. Only measurement
inputs, namespace isolation, diagnostics, statistics and tool pins were added.
The invalid `runner.home` artifact path is corrected on both branches.
The architecture job uses the project toolchain on both sides to control the
compiler version. No Rust source, Cargo.lock or test configuration was changed.

Use distinct `benchmark_cache_namespace` values for the variants, one cold and
at least three warm samples each. Keep `benchmark_target_cache=false` for the
initial comparison. Do not mix manual runs with PR runs. Read the complete
procedure in `tools/ci/README.md` on `ci-bench-optimized`. CFS moved to a separate
workflow there; account for it separately when measuring the whole PR gate.

This branch is for measurements, not for merging.
