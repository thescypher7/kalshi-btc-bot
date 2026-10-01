#!/usr/bin/env bash
# Poll GitHub, gate on tests, deploy, roll back on failure.
# Runs as user `deployer` from kalshi-deploy.timer. Deploys ONLY what is on origin/main.
# A commit that fails the gate is remembered in $STATE/bad_commit and skipped until main moves again.
#
# Everything lives inside main() so bash has parsed the whole script before git can replace this file.
set -euo pipefail

REPO=/opt/kalshi-bot
STATE=/var/lib/kalshi-deploy
SVC=kalshi-logger.service
BRANCH=${DEPLOY_BRANCH:-main}
WT=""

log() { logger -t kalshi-deploy -- "$*"; echo "$*"; }

cleanup() {
  if [ -n "$WT" ]; then
    git -C "$REPO" worktree remove --force "$WT" >/dev/null 2>&1 || true
    rm -rf "$WT"
  fi
}
trap cleanup EXIT

main() {
  export PYTHONDONTWRITEBYTECODE=1
  cd "$REPO"
  git fetch --quiet origin "$BRANCH"

  local prev new
  prev=$(git rev-parse HEAD)
  new=$(git rev-parse "origin/$BRANCH")

  [ "$prev" = "$new" ] && return 0
  if [ -f "$STATE/bad_commit" ] && [ "$(cat "$STATE/bad_commit")" = "$new" ]; then
    return 0   # already rejected this exact commit
  fi

  log "candidate ${new:0:7} (running ${prev:0:7})"

  # 1. Gate: test the candidate in a throwaway worktree; the live checkout is untouched.
  git worktree prune
  WT=$(mktemp -d)
  git worktree add --detach --quiet "$WT" "$new"

  if ! "$REPO/venv/bin/pip" install -q -r "$WT/requirements.txt"; then
    echo "$new" > "$STATE/bad_commit"
    log "REJECTED ${new:0:7}: pip install failed"
    return 1
  fi
  if ! (cd "$WT" && "$REPO/venv/bin/python" -m pytest -q -x -p no:cacheprovider tests); then
    echo "$new" > "$STATE/bad_commit"
    log "REJECTED ${new:0:7}: tests failed"
    return 1
  fi

  # 2. Deploy: fast-forward only (fails loudly if history was rewritten), restart, verify.
  git merge --ff-only --quiet "$new"
  log "deploying ${new:0:7}"
  sudo /usr/bin/systemctl restart "$SVC"
  sleep 20

  if [ "$(systemctl is-active "$SVC" || true)" != "active" ]; then
    echo "$new" > "$STATE/bad_commit"
    log "ROLLBACK to ${prev:0:7}: $SVC not active 20s after restart"
    git reset --hard --quiet "$prev"
    sudo /usr/bin/systemctl restart "$SVC" || true
    return 1
  fi

  rm -f "$STATE/bad_commit"
  log "deployed ${new:0:7} OK"
}

main "$@"; exit $?
