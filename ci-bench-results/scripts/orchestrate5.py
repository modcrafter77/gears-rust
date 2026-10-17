"""Benchmark series v4: one run at a time, each sample a fresh dispatch.

Step 1 follows the optimized warm re-run already in flight on exp2 (run
38646280492 attempt 2) and collects it into exp3. Then the exp4 series runs on
branches carrying the ledger race fix. A failed sample is retried by another
dispatch (labelled warm, since the failed run already filled its namespace).
"""
import json, subprocess, sys, time, datetime, pathlib

REPO = "modcrafter77/gears-rust"
BENCH = r"D:\projects\gears-rust-Modcrafter77-ci-optimization\tools\ci\benchmark.py"
ROOT = pathlib.Path(r"D:\projects\gears-rust-Modcrafter77-ci-optimization\.logs\ci-bench")
EXP3, EXP4 = ROOT / "exp3", ROOT / "exp7"
RETRIES = 2
B = {"name": "baseline", "ref": "ci-bench-baseline-exp4", "ns": "baseline-exp4"}
O = {"name": "optimized", "ref": "ci-bench-optimized-exp7", "ns": "exp7"}
# Blocks, not alternation: both variants' caches together exceed the 10 GB quota.
PLAN = [(O, "warm"), (O, "warm"), (O, "warm"), (B, "warm"), (B, "warm"), (B, "warm")]
LOG = EXP4 / "orchestrate.log"


def log(msg):
    line = f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} {msg}"
    print(line, flush=True)
    with LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def gh(*args):
    r = subprocess.run(["gh", *args], capture_output=True, text=True, encoding="utf-8")
    if r.returncode:
        raise RuntimeError(f"gh {' '.join(args)}: {r.stderr.strip()}")
    return r.stdout


def run_info(run_id, attempt):
    return json.loads(gh("api", f"repos/{REPO}/actions/runs/{run_id}/attempts/{attempt}"))


def wait(run_id, attempt):
    while True:
        try:
            r = run_info(run_id, attempt)
            if r["status"] == "completed":
                return r["conclusion"]
        except Exception as e:
            log(f"poll error: {e}")
        time.sleep(90)


def failed_jobs(run_id, attempt):
    data = json.loads(gh("api", f"repos/{REPO}/actions/runs/{run_id}/attempts/{attempt}/jobs?per_page=100"))
    return [j["name"] for j in data["jobs"] if j["conclusion"] not in ("success", "skipped")]


def collect(out_dir, name, run_id, attempt, state):
    r = subprocess.run([sys.executable, "-I", BENCH, "collect", "--repo", REPO, "--run", str(run_id),
                        "--attempt", str(attempt), "--cache-state", state, "--output", str(out_dir / f"{name}.json")],
                       capture_output=True, text=True)
    log(f"collect {name} {run_id}#{attempt} {state}: {r.stdout.strip() or r.stderr.strip()}")


def finish(out_dir, name, run_id, attempt, state):
    conclusion = wait(run_id, attempt)
    if conclusion == "success":
        collect(out_dir, name, run_id, attempt, state)
        return True
    log(f"{name}: run {run_id}#{attempt} {conclusion}; failed jobs: {failed_jobs(run_id, attempt)}")
    return False


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


def main():
    EXP4.mkdir(parents=True, exist_ok=True)
    log("optimized: following exp7 cold run 38730484576")
    finish(EXP4, "optimized", 38730484576, 1, "cold")
    log("caches: " + gh("cache", "list", "-R", REPO, "--limit", "50", "--json", "key,sizeInBytes", "--jq", '[.[]|"\(.sizeInBytes/1e6|floor)MB \(.key[0:60])"]|join("; ")').strip())
    for v, state in PLAN:
        for retry in range(RETRIES + 1):
            run_id = dispatch(v)
            log(f"{v['name']}: dispatched {v['ref']} run {run_id} ({state}, ns={v['ns']})")
            ok = finish(EXP4, v["name"], run_id, 1, state)
            log("caches: " + gh("cache", "list", "-R", REPO, "--limit", "50", "--json", "key,sizeInBytes", "--jq", '[.[]|"\(.sizeInBytes/1e6|floor)MB \(.key[0:60])"]|join("; ")').strip())
            if ok:
                break
            state = "warm"
        else:
            log(f"{v['name']}: giving up on this slot after {RETRIES} retries")
    log("series complete")
    for d in (EXP4,):
        r = subprocess.run([sys.executable, "-I", BENCH, "compare", str(d / "baseline.json"), str(d / "optimized.json")],
                           capture_output=True, text=True)
        (d / "compare.txt").write_text(r.stdout + r.stderr, encoding="utf-8")
    log("compare written")


if __name__ == "__main__":
    main()
