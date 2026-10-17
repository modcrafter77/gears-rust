# Delete fork Actions caches except baseline-exp4 rust-cache; tolerate API rate limits.
R=modcrafter77/gears-rust
while :; do
  ids=$(gh api "repos/$R/actions/caches?per_page=100&sort=last_accessed_at&direction=asc" --jq '.actions_caches[]|select(.key|test("bench-baseline-exp4")|not)|.id' 2>/dev/null)
  [ -z "$ids" ] && break
  for id in $ids; do
    until gh api -X DELETE "repos/$R/actions/caches/$id" >/dev/null 2>/tmp/err_$$; do
      grep -qiE "rate limit|403|429" /tmp/err_$$ && { echo "$(date +%T) rate limited, sleeping"; sleep 300; } || break
    done
  done
  echo "$(date +%T) usage $(gh api repos/$R/actions/cache/usage --jq '.active_caches_size_in_bytes/1e9') GB, count $(gh api repos/$R/actions/cache/usage --jq .active_caches_count)"
done
echo done
