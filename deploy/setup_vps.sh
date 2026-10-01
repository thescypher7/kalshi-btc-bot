#!/usr/bin/env bash
# One-time setup for a fresh Ubuntu 22.04/24.04 VPS. Run as root:
#
#   REPO_URL=git@github.com:YOURUSER/kalshi-btc-bot.git bash setup_vps.sh
#
# Safe to re-run. It never reads or writes your Kalshi credentials; you add those afterwards.
# Privileged files (systemd units, sudoers) are installed here by root. The auto-deployer can
# update code, but it cannot change what it is allowed to do as root.
set -euo pipefail

[ "$(id -u)" = 0 ] || { echo "Run as root." >&2; exit 1; }
: "${REPO_URL:?Set REPO_URL, e.g. git@github.com:YOURUSER/kalshi-btc-bot.git}"
APP=/opt/kalshi-bot
ADD_SWAP=${ADD_SWAP:-1}

echo "== packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq git python3 python3-venv python3-pip chrony sqlite3 ca-certificates openssh-client
systemctl enable --now chrony >/dev/null 2>&1 || true

if [ "$ADD_SWAP" = 1 ] && ! swapon --show | grep -q .; then
  echo "== 2 GB swapfile (safety net on a small instance; ADD_SWAP=0 to skip)"
  fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile >/dev/null && swapon /swapfile
  grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

echo "== users"
# kalshi: runs the logger, the only user that can read the Kalshi key.
# deployer: pulls main with a READ-ONLY deploy key, owns the checkout, may restart the logger.
# liaison: the AI agent. Own clone, GitHub token for branches/PRs only, no sudo, no secrets.
id kalshi   >/dev/null 2>&1 || useradd --system --home-dir /var/lib/kalshi-bot --no-create-home --shell /usr/sbin/nologin kalshi
id deployer >/dev/null 2>&1 || useradd --system --create-home --home-dir /home/deployer --shell /bin/bash deployer
id liaison  >/dev/null 2>&1 || useradd --create-home --shell /bin/bash liaison
usermod -aG systemd-journal liaison   # lets the agent read service logs

echo "== directories"
install -d -o kalshi   -g kalshi   -m 750 /var/lib/kalshi-bot
install -d -o root     -g kalshi   -m 750 /etc/kalshi-bot
install -d -o deployer -g deployer -m 755 /var/lib/kalshi-deploy "$APP"

echo "== read-only deploy key for user 'deployer'"
DHOME=$(getent passwd deployer | cut -d: -f6)
runuser -u deployer -- install -d -m 700 "$DHOME/.ssh"
[ -f "$DHOME/.ssh/id_ed25519" ] || runuser -u deployer -- ssh-keygen -q -t ed25519 -N "" \
  -C "kalshi-bot-deployer@$(hostname)" -f "$DHOME/.ssh/id_ed25519"
# Trust-on-first-use for github.com. To be strict, compare against https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/githubs-ssh-key-fingerprints
runuser -u deployer -- sh -c "ssh-keyscan -t ed25519 github.com 2>/dev/null >> '$DHOME/.ssh/known_hosts'"

echo
echo "Add this key on GitHub: repo > Settings > Deploy keys > Add deploy key."
echo "Leave 'Allow write access' UNCHECKED."
echo
cat "$DHOME/.ssh/id_ed25519.pub"
echo
read -rp "Press Enter once the deploy key is added... " _

echo "== code, virtualenv"
if [ ! -d "$APP/.git" ]; then
  runuser -u deployer -- git clone "$REPO_URL" "$APP"
fi
runuser -u deployer -- python3 -m venv "$APP/venv"
runuser -u deployer -- "$APP/venv/bin/pip" install -q --upgrade pip
runuser -u deployer -- "$APP/venv/bin/pip" install -q -r "$APP/requirements.txt"

echo "== systemd units and sudoers (installed by root)"
install -m 644 "$APP/deploy/kalshi-logger.service" "$APP/deploy/kalshi-deploy.service" \
  "$APP/deploy/kalshi-deploy.timer" /etc/systemd/system/
install -m 440 "$APP/deploy/sudoers-deployer" /etc/sudoers.d/kalshi-deployer
if ! visudo -cf /etc/sudoers.d/kalshi-deployer >/dev/null; then
  rm -f /etc/sudoers.d/kalshi-deployer
  echo "sudoers validation failed; removed the file." >&2
  exit 1
fi
[ -f /etc/kalshi-bot/logger.env ] || install -m 640 -o root -g kalshi "$APP/.env.example" /etc/kalshi-bot/logger.env

systemctl daemon-reload
systemctl enable kalshi-logger.service >/dev/null 2>&1
systemctl enable --now kalshi-deploy.timer >/dev/null 2>&1

cat <<'EOF'

== Done. Remaining manual steps (secrets are yours to place):

 1. Put your Kalshi private key in place:
      nano /etc/kalshi-bot/kalshi_private_key.pem
      chown root:kalshi /etc/kalshi-bot/kalshi_private_key.pem && chmod 640 /etc/kalshi-bot/kalshi_private_key.pem
 2. Set your key id:   nano /etc/kalshi-bot/logger.env
 3. Start the logger:  systemctl start kalshi-logger
 4. Watch it:          journalctl -u kalshi-logger -f
                       sqlite3 /var/lib/kalshi-bot/kalshi_log.sqlite 'select count(*) from brti'
 5. Deploy status:     systemctl list-timers kalshi-deploy.timer ; journalctl -t kalshi-deploy

The liaison agent is set up separately; see README.md.
EOF
