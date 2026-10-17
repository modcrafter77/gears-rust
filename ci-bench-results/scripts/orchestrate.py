"""Run the paired CI benchmark series on the fork, one run at a time.

Plan: cold B, cold O, then (warm B, warm O) x WARM. Warm samples are full
re-runs (`gh run rerun`) of the variant's cold run, so they share its cache
namespace. Successful attempts are collected with tools/ci/benchmark.py.
A failed attempt is logged and retried (up to RETRIES) as another warm attempt.
"""
import json, subprocess, sys, time, datetime, pathlib

REPO = "modcrafter77/gears-rust"
BENCH = r"D:\projects\gears-rust-Modcrafter77-ci-optimization\tools\ci\benchmark.py"
OUT = pathlib.Path(sys.argv[1])
WARM, RETRIES = 3, 2
VARIANTS = {
    "baseline": {"ref": "ci-bench-baseline-exp3", "ns": "baseline-exp3", "run": None},
    "optimized": {"ref": "ci-bench-optimized-exp2", "ns": "optimized-exp2", "run": int(sys.argv[2]) if len(sys.argv) > 2 else None},
}
LOG = OUT / "orchestrate.log"
STATE = OUT / "state.json"


def log(msg):
    line = f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} {msg}"
    print(line, flush=True)
    with LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def gh(*args, check=True):
    r = subprocess.run(["gh", *args], capture_output=True, text=True, encoding="utf-8")
    if check and r.returncode:
        raise RuntimeError(f"gh {' '.join(args)}: {r.stderr.strip()}")
    return r.stdout


def run_info(run_id):
    return json.loads(gh("api", f"repos/{REPO}/actions/runs/{run_id}"))


def wait(run_id, attempt):
    while True:
        try:
            r = run_info(run_id)
            if r["run_attempt"] == attempt and r["status"] == "completed":
                return r["conclusion"]
        except Exception as e:  # transient API errors
            log(f"poll error: {e}")
        time.sleep(90)


def failed_jobs(run_id, attempt):
    data = json.loads(gh("api", f"repos/{REPO}/actions/runs/{run_id}/attempts/{attempt}/jobs?per_page=100"))
    return [j["name"] for j in data["jobs"] if j["conclusion"] not in ("success", "skipped")]


def collect(name, run_id, attempt, state):
    out = OUT / f"{name}.json"
    r = subprocess.run([sys.executable, "-I", BENCH, "collect", "--repo", REPO, "--run", str(run_id),
                        "--attempt", str(attempt), "--cache-state", state, "--output", str(out)],
                       capture_output=True, text=True)
    log(f"collect {name} #{attempt} {state}: {r.stdout.strip() or r.stderr.strip()}")


def dispatch(v):
    since = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=30)
    gh("workflow", "run", "ci.yml", "-R", REPO, "--ref", v["ref"], "-f", f"benchmark_cache_namespace={v['ns']}")
    for _ in range(30):
        time.sleep(10)
        runs = json.loads(gh("run", "list", "-R", REPO, "--workflow", "ci.yml", "--branch", v["ref"],
                             "--event", "workflow_dispatch", "--limit", "1", "--json", "databaseId,createdAt"))
        if runs and datetime.datetime.fromisoformat(runs[0]["createdAt"].replace("Z", "+00:00")) > since:
            return runs[0]["databaseId"]
    raise RuntimeError("dispatched run not found")


def sample(name, state):
    v = VARIANTS[name]
    for retry in range(RETRIES + 1):
        if v["run"] is None:
            v["run"] = dispatch(v)
            attempt = 1
            log(f"{name}: dispatched run {v['run']} ({state})")
        elif state == "cold" and retry == 0:
            attempt = run_info(v["run"])["run_attempt"]
            log(f"{name}: following existing run {v['run']} attempt {attempt} ({state})")
        else:
            prev = run_info(v["run"])["run_attempt"]
            gh("run", "rerun", str(v["run"]), "-R", REPO)
            attempt = prev + 1
            while run_info(v["run"])["run_attempt"] != attempt:
                time.sleep(10)
            log(f"{name}: rerun {v['run']} attempt {attempt} ({state})")
        STATE.write_text(json.dumps(VARIANTS, indent=2), encoding="utf-8")
        conclusion = wait(v["run"], attempt)
        if conclusion == "success":
            collect(name, v["run"], attempt, state)
            return
        log(f"{name}: run {v['run']} attempt {attempt} {conclusion}; failed jobs: {failed_jobs(v['run'], attempt)}")
        state = "warm"  # any retry runs on a cache the failed attempt already warmed
    log(f"{name}: giving up on this slot after {RETRIES} retries")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    plan = [("optimized", "cold"), ("baseline", "cold")] + [("baseline", "warm"), ("optimized", "warm")] * WARM
    for name, state in plan:
        sample(name, state)
    log("series complete")
    r = subprocess.run([sys.executable, "-I", BENCH, "compare", str(OUT / "baseline.json"), str(OUT / "optimized.json")],
                       capture_output=True, text=True)
    (OUT / "compare.txt").write_text(r.stdout + r.stderr, encoding="utf-8")
    log("compare written")


if __name__ == "__main__":
    main()
