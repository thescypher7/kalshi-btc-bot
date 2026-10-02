# kalshi-btc-bot

Read-only data logger for markets, plus the tooling
to run it on a VPS and an AI "liaison" agent that manages the code between the VPS and GitHub.

**Status: logging only. Nothing here places orders.** The goal is to collect enough data (BRTI ticks,
Kalshi order books and settlements) to measure whether any edge survives fees before writing a trader.

```
logger/kalshi_logger.py   async websocket logger -> SQLite (tables: raw, brti)
logger/retention.py       hourly job: archives old rows to gzip, keeps the live DB small
analysis/                 offline checks on the logged data (read-only)
tests/                    offline tests (no network, no credentials)
deploy/                   systemd units, deploy script, one-time VPS setup
CLAUDE.md                 standing rules for the liaison agent
.claude/settings.json     the agent's permission rules
```

## Who can do what on the VPS

| User | Purpose | Can | Cannot |
|---|---|---|---|
| `kalshi` | runs the logger | read the Kalshi key, write `/var/lib/kalshi-bot` | log in, use sudo |
| `deployer` | auto-deploys `main` | pull with a **read-only** deploy key, restart the logger (one sudo rule) | push to GitHub |
| `liaison` | the AI agent | branches + PRs via a repo-scoped token, read service logs | touch `main`, read secrets, sudo |

Code flow: **liaison pushes a branch -> you review and merge the PR -> deployer tests and deploys `main`
(rolls back if the logger isn't healthy)**. The agent can propose anything but ship nothing.

## Setup

### 1. GitHub (you, in the browser)
1. Create a **private** repo `kalshi-btc-bot` under your account (empty, no README).
2. Push this folder to it from your computer:
   ```
   git init -b main && git add . && git commit -m "Initial import"
   git remote add origin git@github.com:YOURUSER/kalshi-btc-bot.git && git push -u origin main
   ```
3. Repo **Settings > Rules > Rulesets > New branch ruleset**: target `main`, enforcement *Active*,
   **no bypass actors**. Enable: *Restrict deletions*, *Block force pushes*, *Require a pull request
   before merging*. (Leave "required approvals" at 0 if you're the only human; the point is that
   nothing reaches `main` except through a PR you merge.)
   Then **test it**: try pushing to `main` with the agent's token and confirm it's rejected.

### 2. VPS base setup (as root)
```
git clone https://github.com/YOURUSER/kalshi-btc-bot.git /tmp/kb && cd /tmp/kb/deploy
REPO_URL=git@github.com:YOURUSER/kalshi-btc-bot.git bash setup_vps.sh
```
It prints a public key: add it under repo **Settings > Deploy keys** with write access **unchecked**,
then press Enter. (For a private repo the first clone needs your own access; if it fails, clone with a
temporary token or `scp` the folder, then re-run.)

### 3. Your Kalshi credentials (you, on the VPS)
Follow the printed steps: put the private key in `/etc/kalshi-bot/kalshi_private_key.pem`, set
`KALSHI_KEY_ID` in `/etc/kalshi-bot/logger.env`, then `systemctl start kalshi-logger`.
Use a key from a Kalshi account that holds **no more money than you're willing to lose**, and read-only
scopes if Kalshi offers them.

### 4. Check it is logging
```
journalctl -u kalshi-logger -f
sudo -u kalshi sqlite3 /var/lib/kalshi-bot/kalshi_log.sqlite 'select count(*) from brti'
```

### 5. Agent's GitHub token (you, in the browser)
**Settings > Developer settings > Fine-grained personal access tokens**: resource owner = you,
**only** the `kalshi-btc-bot` repository, 30-90 day expiry, permissions: *Contents: read & write*,
*Pull requests: read & write*, everything else none. Don't paste it into any chat.

### 6. Liaison agent (as root, then as `liaison`)
```
REPO_URL=https://github.com/YOURUSER/kalshi-btc-bot.git GIT_NAME="kalshi liaison" \
GIT_EMAIL="you@example.com" bash /opt/kalshi-bot/deploy/setup_liaison.sh

su - liaison
curl -fsSL https://claude.ai/install.sh | bash      # then sign in with `claude`
cd ~/kalshi-btc-bot && git push origin HEAD           # first push; enter username + token when asked
claude
```
The repo's `CLAUDE.md` and `.claude/settings.json` load automatically.

**RAM warning:** Claude Code's docs list 4 GB+ RAM. This 2 GB instance runs the logger comfortably, but the
agent may be sluggish or get OOM-killed (`MemoryMax` on the logger protects it; the swapfile helps).
If that bites, run the agent on your own computer instead (same repo, same rules), or upsize the VPS
to 4 GB only when you need it.

## Storage and retention
The order-book feed is about 12 GB/day of raw rows. `kalshi-retention.timer` runs `logger/retention.py` hourly as the
`kalshi` user: rows older than `KEEP_HOURS` (6) move to hourly `archive/*.jsonl.gz` files (about 15x smaller, roughly
0.7 GB/day), archives older than `ARCHIVE_DAYS` (7) are deleted, and the oldest go sooner if free disk falls below
`MIN_FREE_GB` (8). Rows are archived and fsynced before they are deleted, and each archived record carries the
SQLite `id`, so after a crash dedupe on `(table, id)`. Set the three variables in `/etc/kalshi-bot/logger.env` to
change them. Download `/var/lib/kalshi-bot/archive/` before the 7 days are up if you want to keep history.
The live database file does not shrink after rows are deleted (SQLite reuses the space); it plateaus at its peak size.

## Checking the data
Kalshi settles a KXBTC15M market Yes when the 60 s BRTI average before close is at least the 60 s average before
open (`floor_strike` = the opening average, `expiration_value` = the closing average). `analysis/validate_settlements.py`
recomputes both from our logged ticks (live DB plus archives) and compares them to Kalshi's published numbers, so we
know the data is trustworthy before measuring any edge:
```
sudo -u kalshi /opt/kalshi-bot/venv/bin/python /opt/kalshi-bot/analysis/validate_settlements.py \
  --db /var/lib/kalshi-bot/kalshi_log.sqlite --csv /tmp/validation.csv
```

## Limits to know about
- The agent's permission rules are guardrails, not a sandbox: shell patterns can be bypassed. The real
  controls are the OS users, the read-only deploy key, the token scope, and the GitHub ruleset above.
- `deploy/*.service`, `*.timer`, `sudoers-deployer` are installed by root and **not** auto-updated.
  After merging a change to them, re-run `setup_vps.sh` as root.
- Only the first run of `setup_vps.sh` needs interaction; it's safe to re-run.
- Disk: raw frames grow roughly 300 MB/day. Check `df -h` weekly or add a prune job once you know what you need.
- Kalshi API details (REST base URL, channel fields) were written from their docs without live testing.
  Run against the **demo** environment first if you can (`KALSHI_WS_URL` / `KALSHI_REST_BASE`).

## Tests
```
python3 -m venv venv && venv/bin/pip install -r requirements.txt && venv/bin/pytest -q
```
