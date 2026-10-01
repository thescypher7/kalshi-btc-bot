#!/usr/bin/env bash
# Prepare the `liaison` user's clone and git identity. Run as root AFTER setup_vps.sh:
#
#   REPO_URL=https://github.com/YOURUSER/kalshi-btc-bot.git GIT_NAME="kalshi liaison" \
#   GIT_EMAIL="you@example.com" bash setup_liaison.sh
#
# It does NOT install Claude Code or handle any token. Those steps are yours (see README.md),
# so the GitHub token never passes through a script or a chat.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "Run as root." >&2; exit 1; }
: "${REPO_URL:?Set REPO_URL (https URL; the token is used over HTTPS)}"
: "${GIT_NAME:?Set GIT_NAME}"
: "${GIT_EMAIL:?Set GIT_EMAIL}"

id liaison >/dev/null 2>&1 || { echo "Run setup_vps.sh first." >&2; exit 1; }
LHOME=$(getent passwd liaison | cut -d: -f6)

runuser -u liaison -- git config --global user.name "$GIT_NAME"
runuser -u liaison -- git config --global user.email "$GIT_EMAIL"
# Credentials are stored in ~/.git-credentials (mode 600, owned by liaison) the first time you push.
runuser -u liaison -- git config --global credential.helper store
runuser -u liaison -- git config --global push.default current

if [ ! -d "$LHOME/kalshi-btc-bot/.git" ]; then
  runuser -u liaison -- git clone "$REPO_URL" "$LHOME/kalshi-btc-bot"
fi
runuser -u liaison -- python3 -m venv "$LHOME/venv"
runuser -u liaison -- "$LHOME/venv/bin/pip" install -q -r "$LHOME/kalshi-btc-bot/requirements.txt"

echo "Liaison clone ready at $LHOME/kalshi-btc-bot. Next: install Claude Code as 'liaison' (README step 6)."
