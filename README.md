# colab-cli

A terminal-first CLI for Google Colab — create runtimes, execute Python on GPUs/TPUs, manage files, and run a multi-account cluster of training jobs. Many implementation patterns are adapted from [`colab-vscode`](https://github.com/googlecolab/colab-vscode) and [`jupyter-kernel-client`](https://github.com/googlecolab/jupyter-kernel-client).

Feature tags used below: `[P1]`–`[P4]` = added by the multi-account/cluster phases on top of upstream; `[up]` = behavior changed from upstream.

- **`colab auth`** — Google OAuth login; **multi-account registry** with an active-account pointer `[P1]`
- **`colab usage`** / **`colab runtime`** — Options, create/destroy/restart runtimes, resource usage
- **`colab exec`** — Python execution with streaming I/O, background mode, image capture
- **`colab shell`** — Interactive per-runtime PTY shells with attach/detach, reconnect-tolerant `[P1]`
- **`colab cluster`** — Cross-account job scheduler: one queue, idle-VM dispatch, auto-provisioning, checkpoint sync, auto-recovery from reclaimed runtimes `[P2]`–`[P4]`
- **`colab port-forward`** — Forward runtime ports locally via HTTP/WebSocket proxy
- **`colab fs`** — Adaptive chunked file transfer up to 8 GiB `[up]`
- **`colab drive`** / **`colab drive-mount`** — Google Drive management and prompt-free mounts

## Requirements

- **Node.js >= 20** `[up]` (was 22-only). On Node 20/21, control-plane REST calls ignore `HTTP(S)_PROXY` env (that flag, `--use-env-proxy`, is Node >= 22 only); a startup warning tells you when this matters. Data-plane proxying (Jupyter WS, transfers) works on all supported versions.
- A Google account with Colab access (or several, for the cluster)
- Network access to: `google.com`, `googleapis.com`, `googleusercontent.com`, `colab.dev`

## Install

```bash
npm install
npm run build
npm link
```

After `npm link`, the `colab` command points to your local build. Run `npm run build` (or keep `npm run dev` running) to pick up code changes — no need to re-link.

Without linking you can always run `node dist/index.js ...` directly from the repo.

## Global options

```
--account <email>   operate on this account instead of the active one `[P1]`
--json              machine-readable output on stdout (progress/prompts → stderr)
--verbose           enable verbose logging
```

---

## Multi-account `[P1]`

The CLI keeps a registry of Google accounts; commands act on the **active** account unless `--account` overrides it:

```bash
colab auth login                      # add an account via browser OAuth (becomes active)
colab auth login --account <email>    # re-auth an existing registered account
colab auth list                       # list registered accounts (* marks active)
colab auth switch <email>             # change active account (lazy; token refresh deferred)
colab auth logout [--account <email>] # sign out; next registered account becomes active
colab auth status                     # active-account session status
```

> State: `~/.config/colab-cli/accounts/<email>/{auth.json,servers.json}` per account, plus an accounts registry file. Legacy single-account installations are migrated automatically on first use.

Most commands accept the global `--account <email>` (runtime/fs/exec/shell/drive/...). `colab cluster` is account-agnostic and touches every registered account — see below.

### Authentication (detail)

```bash
colab auth login / status / logout    # as above; login defaults to the fresh-account flow
```

---

## Runtimes

```bash
colab runtime available                          # accelerators/shapes for the account
colab runtime versions                           # runtime versions and environment details
colab runtime create --accelerator CPU
colab runtime create --accelerator T4 --shape standard
colab runtime create --accelerator v6e-1 --shape high-ram
colab runtime create --accelerator T4 --runtime-version 2025.10
colab runtime create --accelerator T4 --kernel r

colab runtime list                               # active runtimes (per account)
colab runtime resources [-e <endpoint>]          # RAM / disk / GPU usage
colab runtime destroy --endpoint <ep> [--account <email>]
colab runtime restart --endpoint <ep>            # restart kernel, keep the VM
colab usage                                      # subscription tier + CCU usage (top-level)
```

> Runtime records: `~/.config/colab-cli/accounts/<email>/servers.json`

---

## Cluster (multi-account job scheduler) `[P2]`–`[P4]`

One long-lived local daemon owns a queue of shell jobs and farms them out across **all registered accounts' runtimes**: pick an idle VM, or provision a new one under the least-loaded account. If a VM is reclaimed by Colab mid-job, the job is re-queued, re-set-up on a fresh VM, and resumed from its last mirrored checkpoint.

```bash
colab cluster status                 # pool overview (per-account VMs, aliveness) + queue summary
colab cluster submit -c '<cmd>'      # inline job (quote it)
colab cluster submit -f job.json     # job spec file (see below)
colab cluster rehearse setup.sh      # validate a setup script on ONE VM; VM stays idle after
colab cluster list                   # ID / STATUS(+retryN) / PROGRESS / ASSIGNMENT / COMMAND
colab cluster logs <id> [--tail N]   # live snapshot or durable mirror
colab cluster cancel <id>            # cancel queued OR kill a running job's whole process tree
colab cluster ckpts <id>             # list locally mirrored checkpoints
colab cluster shutdown               # stop the daemon (jobs persist; next command restarts it)
```

The daemon auto-starts on the first `cluster` command. State: `~/.config/colab-cli/cluster/` (`state.json`, `logs/job-<id>.log`, `checkpoints/job-<id>/`, `daemon.log`).

### Job spec (`-f job.json`)

```json
{
  "name": "train",
  "accelerator": "L4",
  "setup_file": "setup.sh",
  "uploads": [{ "src": "./ds", "dest": "/content/data/ds" }],
  "progress_pattern": "Epoch (\\d+)/(\\d+)",
  "ckpt_glob": "/content/ckpts/*.pt",
  "ckpt_keep": 3,
  "allow_recover": true,
  "max_recoveries": 3,
  "command": "python train.py --resume \"$CLUSTER_RESUME_CKPT\""
}
```

| Field | Meaning | Tag |
|---|---|---|
| `name` / `accelerator` | Display name; hardware request for a newly provisioned VM | |
| `setup_file` | Local script; runs once per VM, cached by content hash per endpoint | |
| `uploads` | Local→VM files pushed before the shell starts | |
| `progress_pattern` | Regex; last match on stdout shows as PROGRESS in `cluster list` (capture groups joined with `/`) | `[P3]` |
| `ckpt_glob` | VM-side glob scanned every ~60s; matches are chunked-downloaded locally | `[P3]` |
| `ckpt_keep` | Local ckpt retention, pruned by remote mtime recency (default 3) | `[P3]` |
| `allow_recover` | `false` = fail instead of auto-recovering a reclaimed VM (default true) | `[P4]` |
| `max_recoveries` | Give up after N reclaimed VMs (default 3) | `[P4]` |

### Behavior notes

- **Log durability**: a running job's stdout is mirrored locally every tick; `logs` still works after the VM is gone. `[P2]`
- **Setup rehearsal**: `rehearse` runs only your setup script on one VM so you can iterate quickly; a VM that passed setup with the same content skips re-running it.
- **Checkpoint sync** `[P3]`: a final forced scan at job end prevents losing the tail checkpoint; files already pruned by `ckpt_keep` are never re-fetched.
- **Auto-recovery** `[P4]`: 3 consecutive probe failures (~30s) mean the VM is gone (reclaim, host sleep, …). The job re-queues (`retry1` shows in `cluster list`), re-runs setup on a fresh VM, re-uploads the latest checkpoint, and re-runs the command with:
  - `CLUSTER_RESUME=1` (`0` on the first run) and `CLUSTER_RESUME_CKPT=<remote path>` (empty when no checkpoint mirrored yet)
  - Whether training actually resumes is **your script's job** — the CLI only delivers the file and the signal.
- **Fault fencing**: a dispatch failure puts that VM on a 5-minute bad-list and falls back to another idle VM (or a fresh provision) instead of retrying the same dead end. Idle pool VMs are health-swept; dead ones are forgotten automatically.

---

## Execute Code

Run inline Python or a file:

```bash
colab exec "print('hello')"
colab exec -f script.py
colab exec -e <endpoint> "..."    # target a specific runtime
```

For code with nested quotes or shell metacharacters, pipe via stdin to bypass local escaping:

```bash
colab exec <<'EOF'
import os
print(f"hello {os.environ['USER']}")
EOF
```

Save image outputs (PNG/JPEG/GIF/SVG) to a directory:

```bash
colab exec -o ./plots "..."    # saved path printed to terminal
```

> Default image output dir: `~/.config/colab-cli/outputs/<serverId>/`

- `input()` and `getpass` prompts are forwarded to the terminal; password prompts suppress echo.
- Ctrl+C sends a kernel interrupt — also works during `input()` prompts. A second Ctrl+C force-exits.
- Exceptions exit non-zero.
- If code calls `drive.mount()`, the CLI handles the OAuth consent flow. With `colab drive-mount` pre-configured, the mount returns immediately.

### Background Execution

```bash
colab exec --background "import time; time.sleep(60)"    # returns exec ID immediately
colab exec list                                          # list all executions
```

Statuses: `running`, `done`, `error`, `crashed`, `input` (waiting for stdin), `auth` (waiting for browser OAuth).

```bash
colab exec attach 1             # replay buffered output then stream live (blocks until done)
colab exec attach 1 --no-wait   # snapshot buffered output and exit
colab exec attach 1 --tail 20   # last 20 outputs only
```

```bash
colab exec send 1 --stdin "yes"    # send stdin to a running execution
colab exec send 1 --interrupt       # Ctrl+C equivalent
```

```bash
colab exec clear      # clear all completed executions
colab exec clear 3    # clear a specific execution by ID
```

- Running and input-waiting executions are preserved; only completed entries are removed.
- The kernel is serial — only one execution runs at a time. Use `colab shell` for side tasks during a long exec.
- When status is `auth`, any `attach` variant prints the stored OAuth URL; the daemon auto-retries credential propagation every 5s after the browser flow completes.

---

## Interactive Shell `[P1: rewritten as per-shell PTY relays]`

```bash
colab shell              # interactive terminal; press Ctrl+\ to detach
colab shell -b           # start detached, prints shell ID
colab shell list         # list sessions
colab shell attach 1 [--no-wait] [--tail 40]
colab shell send 1 --data 'ls -la\n'      # raw data; escape sequences ok
colab shell send 1 --signal INT|EOF|TSTP|QUIT
colab shell close 1                        # kill the session `[P1]`
```

Each shell is an independent PTY relay process on the VM (bash exit closes exactly that shell; transport hiccups trigger an automatic reconnect window of up to 120 s rather than killing it). `[P1]`

Pipe complex commands via stdin with a quoted heredoc to avoid local expansion:

```bash
colab shell send 1 <<'EOF'
export LD_LIBRARY_PATH=$(python -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))'):$LD_LIBRARY_PATH
EOF
```

---

## Port Forwarding

Forward a runtime port to your local machine via an HTTP/WebSocket reverse proxy — no ngrok or runtime-side agent needed.

```bash
colab port-forward create 7860                 # REMOTE (binds 127.0.0.1 by default)
colab port-forward create 18080:7860           # LOCAL:REMOTE
colab port-forward create 0.0.0.0:18080:7860   # HOST:LOCAL:REMOTE
colab port-forward create 7860 --tls           # local listener over HTTPS
colab port-forward list
colab port-forward close 1
colab port-forward close --all
```

`pf` is a shorter alias for `port-forward`.

- HTTP and WebSocket only — raw TCP (PostgreSQL, Redis, SSH, gRPC) is not supported.
- Forwards live as long as the daemon; a destroyed runtime or killed daemon clears them.

---

## File Transfer `[up: adaptive chunking, 8 GiB cap]`

```bash
colab fs upload ./data.csv
colab fs upload ./model.bin -r content/models/model.bin
colab fs download content/results.json
colab fs download content/output.bin -o ./local-output.bin
```

| File size | Strategy |
|-----------|----------|
| <= 20 MiB | Single REST request |
| 20 MiB – 8 GiB | Chunked; chunk size (4/8/16 MiB), lane count (3/6/12) and per-chunk timeout auto-tuned by a 1 MiB bandwidth probe at transfer start |
| > 8 GiB | Use `colab drive upload` / `colab drive download` |

Both `fs` and cluster job `uploads` share this adaptive path.

---

## Google Drive

Drive uses a **separate OAuth flow** from Colab auth:

```bash
colab drive login / logout / status
colab drive list [folder-id]                    # default: root; `shared` = shared with me
colab drive info <item-id>
colab drive upload <local-path> [-p <id>]       # resumable for >5 MiB
colab drive download <file-id> [-o <path>]
colab drive mkdir <name> [-p <id>]
colab drive delete <file-id> [--permanent]      # default: trash
colab drive copy <file-id> [--to <id>] [--name <name>]
colab drive rename <item-id> <new-name>
colab drive move <item-id> --to <folder-id>
```

All commands use **file/folder IDs** (not names) — use `drive list` to find them.

- **Resumable upload**: files >5 MiB use Google's resumable protocol; re-run to resume an interrupted upload.
- **MD5 dedup**: skips upload if an identical file exists in the target folder.
- Shared-Drive rules: `move` across Shared-Drive boundaries falls back to copy / is rejected as documented per command; `delete` rejects files you don't own.

### Custom OAuth Credentials

If you hit Drive quota limits, set your own GCP OAuth client (`COLAB_DRIVE_CLIENT_ID` / `COLAB_DRIVE_CLIENT_SECRET`) and re-run `colab drive login`.

---

## Automatic Drive Mounting

```bash
colab drive-mount login          # authorize once (opens browser)
colab drive-mount [-e <endpoint>]
colab drive-mount status / logout
```

Drive mounts at `/content/drive`; later `drive.mount()` calls detect the existing mount and return immediately.

---

## JSON Output (Scripting)

Most commands support `--json` for machine-readable output (`exec` and `shell` excluded). Login commands in `--json` mode are non-blocking — they emit an `auth_required` event with the OAuth URL and exit; poll the matching `status --json` after completing the browser flow.

```bash
colab drive mkdir models -p "$PARENT_ID" --json
colab auth login --json
```

Command failures emit `{"error":"..."}` with a non-zero exit code.

---

## Notes

- `runtime destroy` removes both the live assignment and the locally stored record, and gracefully shuts down the associated daemon.
- If Colab returns `412` or `503` during runtime creation, that is usually a backend quota/capacity issue, not a local transport failure.
- A shell that exits normally is now reported `closed` within ~1 s (previously up to 2 min of reconnect budget). Cluster job completion detection inherits this. `[up]`
- For design docs, phase reports and incident notes, see `docs/`.
