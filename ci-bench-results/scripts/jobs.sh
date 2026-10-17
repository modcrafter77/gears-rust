# usage: jobs.sh repo run_id -> name \t conclusion \t queue_s \t run_s
gh api --paginate "repos/$1/actions/runs/$2/jobs?per_page=100" --jq '.jobs[]|[.name,.conclusion,(.created_at|fromdate) as $c|(.started_at|fromdate) as $s|(if .completed_at then (.completed_at|fromdate) else null end) as $e|($s-$c),(if $e then $e-$s else -1 end)]|@tsv'
