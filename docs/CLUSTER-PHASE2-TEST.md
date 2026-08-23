# CLUSTER Phase 2 測試手冊(Test Handbook)

> 目標:驗證 cluster job scheduler(submit/rehearse/logs/cancel/status)+ log mirror + setup 防重跑 + uploads。
> 執行者:另一隻 agent(或人類)。逐條跑,每條有 **預期結果**,都不符合才回報失敗。
>
> 前置:Phase 2 已 merge 進 `phase-2` branch(commit `209fc3f`)。以下全部 command 都在 repo 根目錄執行。

---

## 0. 測試前準備

```bash
cd /media/codingbear/code/colab-test/colab-cli
nvm use 22          # 確保 Node 22
npm run build       # 必須 0 error TS
ls dist/cluster/    # 應看到 daemon.js client.js pool.js state.js upload.js
```

帳號環境:
- 必須至少有一個帳號 `auth login` 過且 token 未過期(測試建議 `codingbear.ai.07@gmail.com`,或任一 .02–.06)
- 確認:`node dist/index.js auth list` 看到該帳號

防呆:如果你看到「Not logged in」漫天飛,先 `auth list` / 重新 login,**不要怪 scheduler**。

**每個測試結束**建議把產生的 runtime destroy,避免免費帳號被 Colab 限流:
`node dist/index.js runtime list` → 對測試開出來的 endpoint `runtime destroy -e <endpoint> --account <email>`

---

## 測試編號對照

| ID | 測什麼 |
|---|---|
| T1 | CLI 註冊與 cluster daemon 自動 spawn |
| T2 | 最簡單 submit -c(無 setup)→ done |
| T3 | rehearse:壞腳本 → failed,log 看得到錯 |
| T4 | rehearse:好腳本 → done,vmSetup 記 hash |
| T5 | 同 VM 重用時 setup 跳過 |
| T6 | submit -f json:uploads 在 command 前跑完 |
| T7 | 失敗分類:setup fail vs command fail 要分得出來 |
| T8 | cancel 排隊中 / 執行中 job |
| T9 | logs 在 VM destroy 後依然可讀 |
| T10 | 兩個 job 連續 submit(第二個要排隊或開新 VM) |

由淺入深,**T1 紅了就不要往下**,先修基礎。

---
## T1 — CLI 註冊 + cluster daemon 自動 spawn

```bash
node dist/index.js cluster --help
# 預期:看到 status/submit/rehearse/list/logs/cancel 六個子指令
node dist/index.js cluster status
# 預期:印出所有已登入帳號的 pool,最後一行 "VMs: x idle / y busy / z down"
# 同時 ~/.config/colab-cli/cluster/cluster.pid 產生、daemon.sock 存在
ls ~/.config/colab-cli/cluster/
# 預期:state.json  daemon.log  cluster.pid  cluster.sock  logs/
```

**通過標準**:status 跑完沒有 error;`pgrep -fa cluster/daemon` 能看到一隻長駐 process;state.json 有 `{"nextJobId":N,"jobs":[],"vmSetup":{}}` 結構。

---

## T2 — 最簡單 submit(無 setup/upload),全 happy path

```bash
node dist/index.js cluster submit -c 'echo T2_HELLO; date; sleep 5' -n t2-smoke
# 預期:印 "Queued job N (t2-smoke)"
```

等 90 秒後(第一次沒有 idle VM 時要含 provision 時間,~1 分鐘):

```bash
node dist/index.js cluster list | tail -1
# 預期:N  running  t2-smoke  <account> @ <endpoint> shell=S  echo T2_HELLO...
```

再等它跑完(command 有 sleep 5,所以從 running 到 done 要 ~2 分,因為 shell 關了要等 reconnect window):

```bash
node dist/index.js cluster list | tail -1
# 預期:N  done  t2-smoke
node dist/index.js cluster logs N | tail -10
# 預期:看到 T2_HELLO、日期字串、最後 "/content# exit"
ls -la ~/.config/colab-cli/cluster/logs/job-N.log
# 預期:存在,大小 > 0
```

**通過標準**:done;logs 有 T2_HELLO;log file 存在。**失敗典型**:一直是 provisioning = relay 404 / kernel exec 卡；一直是 queued = 所有帳號 Not logged in。

---

## T3 — rehearse 壞腳本 → 必須 failed,且 log 看得懂

```bash
cat > /tmp/rehearse-bad.sh <<'EOF'
echo SETUP_STARTING
ls /this-path-does-not-exist
touch /tmp/SETUP_DONE_MARKER
EOF
node dist/index.js cluster rehearse /tmp/rehearse-bad.sh -n t3-bad
# 預期:Queued job N (t3-bad) +setup REHEARSE
```

等 ~3 分鐘：

```bash
node dist/index.js cluster list | tail -1
# 預期:N  failed  t3-bad  ... [setup script failed (see log)]
node dist/index.js cluster logs N | tail -8
# 預期:看到 "SETUP_STARTING"
#       看到 "ls: cannot access '/this-path-does-not-exist'"
#       看不到 __CLUSTER_SETUP_OK__(這是關鍵 — 有看到就是 bug)
#       看不到 /tmp/SETUP_DONE_MARKER 被 touch(因為 set -e 中止)
```

**通過標準**:failed;log 裡**沒有** `__CLUSTER_SETUP_OK__`;`grep "SETUP_DONE_MARKER"` log 檔裡應該只有在「腳本本身被 cat 出來」的行而不該有「執行結果」的登記。

**最常踩的雷**：如果在 log 裡看到 `__CLUSTER_SETUP_OK__` 出現,就是 heredoc echo 污染,要回報這是 bug(Phase 2 使用 kernel exec 寫檔正是不讓 echo 污染)。

---
## T4 — rehearse 好腳本 → done + vmSetup 記錄

```bash
cat > /tmp/rehearse-good.sh <<'EOF'
echo SETUP_IS_RUNNING_NOW
mkdir -p /content/t4-probe
echo probe > /content/t4-probe/marker.txt
EOF
node dist/index.js cluster rehearse /tmp/rehearse-good.sh -n t4-good
```

等 ~3 分鐘：

```bash
node dist/index.js cluster list | tail -1
# 預期:N  done  t4-good
node dist/index.js cluster logs N | tail -8
# 預期:看到 SETUP_IS_RUNNING_NOW、__CLUSTER_SETUP_OK__、__CLUSTER_JOB_OK__
```

驗證 vmSetup:

```bash
node -e "const s=require(process.env.HOME+'/.config/colab-cli/cluster/state.json'); console.log(JSON.stringify(s.vmSetup,null,2))"
# 預期:至少一個 endpoint 進來,hash 是 install-good.sh 內容的 sha-like
```

**通過標準**:done;`vmSetup[endpoint].hash` 存在。**失敗典型**:一直 running → 可能腳本在等待 input(TTY prompt),基礎 bash 勿用 read/select。

---

## T5 — 同 VM 重複 submit 要跳過 setup

```
前提:T4 的 VM 還活著
```

```bash
# 給同一台 VM 再丟一個 job,setup_file 一樣指到 /tmp/rehearse-good.sh
cat > /tmp/t5-spec.json <<'EOF'
{
  "name": "t5-skipsetup",
  "setup_file": "/tmp/rehearse-good.sh",
  "command": "cat /content/t4-probe/marker.txt"
}
EOF
node dist/index.js cluster submit -f /tmp/t5-spec.json
```

等 ~2 分鐘(這次不用等 setup,跑很快):

```bash
node dist/index.js cluster logs <N> | tail -10
# 預期:有 marker.txt 的內容("probe")
# 預期:❗ "SETUP_IS_RUNNING_NOW" ❌ 不出現 → setup 被跳過(vmSetup hit)
node -e "const s=require(process.env.HOME+'/.config/colab-cli/cluster/state.json'); const j=s.jobs.find(x=>x.id===N); console.log('accountId:', j.accountId, 'endpoint:', j.endpoint)"
# 預期:endpoint 跟 T4 同一台
```

**通過標準**:done;log 裡沒有 SETUP_IS_RUNNING_NOW;有 probe 內容。
**反向證明**:如果你故意改 install.sh 內容(例如加一行 echo),hash 變了,下一個 job 應該**重新**跑 setup。可以做可選。

---

## T6 — uploads:本地檔案傳到 VM,且在 command 之前完成

```bash
dd if=/dev/zero of=/tmp/t6-data.bin bs=1M count=80  # 80 MiB,觸發 chunked
sha256sum /tmp/t6-data.bin | awk '{print $1}' > /tmp/t6-sha.txt
cat > /tmp/t6-spec.json <<'EOF'
{
  "name": "t6-upload",
  "uploads": [{"src": "/tmp/t6-data.bin", "dest": "/content/data/"}],
  "command": "sha256sum /content/data/t6-data.bin"
}
EOF
node dist/index.js cluster submit -f /tmp/t6-spec.json
```

等完成(80MiB 上傳約 30~60 秒):

```bash
node dist/index.js cluster logs <N> | tail -10
# 預期:
#   - 看到 "uploading /tmp/t6-data.bin -> /content/data/" 開頭的串(不保證在 log 裡,但 daemon.log 會有)
#   - VM 上的 sha256 跟你本機 /tmp/t6-sha.txt 一致
diff <(awk '{print $1}' /tmp/t6-sha.txt) <(awk '{print $1}' <(node dist/index.js cluster logs <N>))
# 預期:兩個 hash 完全一致
```

**通過標準**:done;sha256 完全相同。
**若不及**:logs 尾巴如果只有命令而沒有 sha256 印出 → upload 失敗；如果 upload 階段 timeout,job 會停在 provisioning,requeued,daemon.log 會有重試行為 — 接受重試一次後成功。

---
## T7 — 失敗分類:setup 壞 vs command 壞 要分得出來

兩隻 job 背靠背測:

```bash
# 7a: setup OK,但 command 爛
cat > /tmp/t7a-spec.json <<'EOF'
{ "name": "t7a-cmdfail", "setup_file": "/tmp/rehearse-good.sh", "command": "python3 -c 'import sys; print(1/0)'" }
EOF
node dist/index.js cluster submit -f /tmp/t7a-spec.json

# 7b: setup 就爛,command 不該被執行
cat > /tmp/t7b-spec.json <<'EOF'
{ "name": "t7b-setupfail", "setup_file": "/tmp/rehearse-bad.sh", "command": "echo SHOULD_NEVER_SEE_THIS" }
EOF
node dist/index.js cluster submit -f /tmp/t7b-spec.json
```

等各 ~3 分鐘：

```bash
node dist/index.js cluster list | tail -3
# 預期 7a: failed  +  error含 "command exited without the completion marker"
#          logs 裡 有 __CLUSTER_SETUP_OK__(setup 過了)沒有 JOB_OK
# 預期 7b: failed  +  error含 "setup script failed"
#          logs 裡 沒有 __CLUSTER_SETUP_OK__,也沒有 SHOULD_NEVER_SEE_THIS
```

**通過標準**：兩個都 failed，但 error 訊息不同，且 log 裡的 marker 跟上述預期一致。

---
## T8 — cancel:排隊中 vs 執行中

```bash
# 8a: cancel 排隊中
node dist/index.js cluster submit -c 'sleep 9999' -n t8a
node dist/index.js cluster cancel <8a_id>
node dist/index.js cluster list | tail -1
# 預期: status=cancelled  endedAt 有值  從不會變 running

# 8b: cancel 執行中(要殺掉 VM 上的整棵 process tree)
node dist/index.js cluster submit -c 'sleep 9999; echo SURVIVED' -n t8b
# 等它變 running(需 ~1 分)
node dist/index.js cluster list | tail -1   # 確認是 running
node dist/index.js cluster cancel <8b_id>
# 直接進 VM 驗:沒有 sleep 還在跑
node dist/index.js exec --account <該 account> -e <endpoint> \
  'import subprocess; print(subprocess.run(["pgrep","-f","sleep 9999"],capture_output=True,text=True).stdout)'
# 預期: 空白輸出(sleep 已被殺光)
node dist/index.js cluster list | tail -1
# 預期: 8b cancelled
```

**通過標準**:
- 8a：直接 cancelled,VM 上一個 shell 都沒起來
- 8b：執行中 cancel 後,VM 上沒有 sleep process 殘留，`SURVIVED` 沒被印出

---
## T9 — VM 被回收/手動 destroy 後,log 依然可讀

```bash
node dist/index.js cluster submit -c 'for i in $(seq 1 60); do echo TICK_$i; sleep 1; done' -n t9
# 等 running 起來(要 TICK_1 至少印一次)

# 直接 destroy 那台 VM(模擬 VM 被回收)
node dist/index.js runtime destroy -e <endpoint> --account <account>

# 等 ~30 秒讓 cluster daemon 發現 VM 失蹤
sleep 35
node dist/index.js cluster list | tail -1
# 預期: failed  +  error 含 "runtime unreachable"
node dist/index.js cluster logs <N>
# 預期: 還是看得到 TICK_1 ... TICK_k(到 destroy 當下為止的)
#        而且這是從本機 mirror file 讀的,不是從 VM 拿的
```

**通過標準**:job failed、logs 指令**不失敗**而且還印得出 TICK 串。如果 logs 在 VM destroy 後直接報錯或空白 → mirror 沒有寫 → bug。

---

## T10 — 兩個 job 連續 submit(第二個會拿到新 VM,或閒置再分配)

```bash
# 前提:目前 pool 只有 1 台 VM(T1-T9 用的那台)
node dist/index.js cluster submit -c 'sleep 30; echo A_DONE' -n t10-A
node dist/index.js cluster submit -c 'sleep 30; echo B_DONE' -n t10-B
node dist/index.js cluster list
# 預期:
#  t10-A → running(佔第一台)
#  t10-B → 要麼 running(被丟去**第二台**剛 provision 好的 VM)
#          要麼 queued 等 A 結束(看帳號 quota 決定,兩種都 OK)
#          絕對 ❌ 不能跟 A 共用同一台 VM

node dist/index.js cluster status | tail -3
# 預期: 看到 2 個 VM,一個 BUSY job=A,另一個 BUSY job=B(若 B 也 provision 成功)
```

**通過標準**:1 job = 1 VM 不被違反；如果 B 是 queued，則等 A done 後 B 應該重新 dispatch 到同一台（或新）VM。

---

## 收尾（全部測完）

```bash
# 砍掉測試產生的 runtime
node dist/index.js runtime list
node dist/index.js runtime destroy -e <every-endpoint> --account <該 account>

# 檢查沒有 orphan relay
node dist/index.js cluster status
# 預期: 全部 (no stored runtimes) 或都 DOWN

# 停掉 cluster daemon
node dist/index.js cluster shutdown
kill $(cat ~/.config/colab-cli/cluster/cluster.pid) 2>/dev/null  # 備援
```

## 測試報告格式（寫到 `docs/CLUSTER-PHASE2-TEST-REPORT.md`)

| ID | 結果 | 證據（指令+輸出節錄） | 備注 |
|---|---|---|---|
| T1 | ✅/❌ | ... | ... |
| ... | | | |

全綠才算 Phase 2 可收；有紅的標出 expected/actual + 你猜的根因，別自己偷偷修。

## Round 2 — 本輪 code 變更(2026-08-23 修 T6/T8 + adaptive upload)

以下 code 全部被動過，**即使測過的 T# 也要重新跑**:

| 檔案 | 變更 | 影響面 |
|---|---|---|
| `src/cluster/state.ts` | `updateJob` 加 terminal-state 守衛(done/failed/cancelled 不能被覆寫回 active) | 所有 cancel/failed 流程 |
| `src/cluster/daemon.ts` | dispatch 完開 shell 後若偵測到已 cancelled → 立即 shellClose | T8 cancel-related |
| `src/jupyter/contents-client.ts` | `putFileBase64` timeout 依 payload 縮放,或吃呼叫方傳入的 timeoutMs | **全部上傳**(含原生 `colab fs upload`) |
| `src/transfer/common.ts` | `CHUNKED_MAX_BYTES` 500MiB → **8GiB**;`normalizeRemotePath` 認目錄式 dest(自動接 basename) | 大檔上傳(原本是硬擋)+ 所有 upload CLI 的 dest 處理 |
| `src/transfer/upload.ts` | 新 `probeUplink`(1MiB) + `planForRate`:chunked 路徑動態決定 chunk size / 並行數 / timeout | **所有 chunked 上傳**(20MiB~8GiB) |
| `src/cluster/upload.ts` | cluster 自動上傳拔掉固定 chunk 參數,換用 probe 自適應;dest 是目錄的處理改由底層接手 | job 的 uploads 流程 |

### Round 2 新增測試

---
<!-- R2-ANCHOR -->

### T11 — 原生 CLI `fs upload` chunked + 自適應(驗測速路徑)

T6 修的不只是 cluster，底層 transfer 也動了。原生 CLI 也要驗。

```bash
dd if=/dev/urandom of=/tmp/t11.bin bs=1M count=100   # 100 MiB,確保走 chunked
sha256sum /tmp/t11.bin | awk '{print $1}' > /tmp/t11.sha

time node dist/index.js fs upload /tmp/t11.bin \
  --account <email> -e <endpoint> --remote-path /content/t11/t11.bin

node dist/index.js exec --account <email> -e <endpoint> \
  'import hashlib; print(hashlib.sha256(open("/content/t11/t11.bin","rb").read()).hexdigest())'
diff <(cat /tmp/t11.sha) <(上一步的印出)   # 必須一致
```

**驗重點**:
- 上傳中`/content/.colab-transfer-tmp/` 先冒出 `__uplink_probe__.bin` 再消失（另開終端機瞟）
- chunk 數量手機網 vs 固網會不同(自適應)— **都算正常**
- sha256 一致

---

### T12 — 目錄式 dest(`--remote-path` 給資料夾,`uploads dest` 給資料夾)

normalizeRemotePath 修改了，兩種 dest 型態都要過:

```bash
echo DIRTEST > /tmp/t12.txt

# CLI 版本
node dist/index.js fs upload /tmp/t12.txt --account <email> -e <endpoint> --remote-path /content/dirtest/
# 預期: 上傳到 /content/dirtest/t12.txt(底層自動接 basename),不是 404

# cluster job 版本
cat > /tmp/t12-spec.json <<'EOF'
{"name":"t12-dirdest","uploads":[{"src":"/tmp/t12.txt","dest":"/content/data/"}],"command":"ls -la /content/data/t12.txt"}
EOF
node dist/index.js cluster submit -f /tmp/t12-spec.json
# 等 done 後 logs 應看到 /content/data/t12.txt 的 -rw-r--r-- 行
```

**驗重點**:**兩邊都要成功**。修前在 cluster 我們有 workaround 補basename,現在交給底層;CLI 沒有 workaround,底層修後才通。

---

### T13 — 大檔上限引擎:500MiB 過界 → 8GiB 放行的邏輯

直接對 CLI 下，不傳(不想燒頻寬也可以 dry-run 行為測試—看 chooseStrategy 用 node -e 即可):

```bash
node --input-type=module -e "
import { chooseStrategy } from './dist/transfer/common.js';
console.log('600MiB →', chooseStrategy(600*1024*1024)); // 預期 chunked(以前會是 drive)
console.log('9GiB   →', chooseStrategy(9*1024**3));      // 預期 drive(未實作,會報錯)
console.log('200MiB →', chooseStrategy(200*1024*1024)); // 預期 chunked
"
```

**驗重點**:600MiB 走 chunked。如果你想真的傳一個 600MiB 的檔確認沒被擋,在固網機器上做,手機網別試。

---
### T14 — T8 race 回歸(修後驗證)

```bash
# submit 後 0.3 秒內 cancel(手速要快,或用腳本)
JID=$(node dist/index.js cluster submit -c 'sleep 9999' -n t14-race | grep -oP 'job \K\d+')
sleep 0.3
node dist/index.js cluster cancel $JID

sleep 150
node dist/index.js cluster list | tail -1
# 預期：
#  - status = cancelled(不再變成 running)
#  - VM 上沒有 sleep 9999 殘留:
node dist/index.js exec --account <a> -e <e> \
  'import subprocess; print("STRAY" if subprocess.run(["pgrep","-f","sleep 9999"],capture_output=True).returncode==0 else "CLEAN")'
# 預期: CLEAN
```

**驗重點**：這是 regression 測試；**不能**看到 list 變 running、不能看到 CLEAN 變 STRAY。

---

### T15 — cancel running job 仍然動(修 T8 不要弄壞 T8b)

重跑 T8b 那組:

```bash
node dist/index.js cluster submit -c 'sleep 9999; echo SURVIVED' -n t15
# 等 running
node dist/index.js cluster cancel <id>
# 預期: cancelled;exec pgrep 是 CLEAN;SURVIVED 沒被印
```

---

### T16 — 連續 upload 一致性（probe 不會造成 race/殘留）

連續 3 個 upload，每個 30MiB:

```bash
dd if=/dev/urandom of=/tmp/t16-1.bin bs=1M count=30
dd if=/dev/urandom of=/tmp/t16-2.bin bs=1M count=30
dd if=/dev/urandom of=/tmp/t16-3.bin bs=1M count=30

for i in 1 2 3; do
  time node dist/index.js fs upload /tmp/t16-$i.bin --account <a> -e <e> \
    --remote-path /content/probe/t16-$i.bin
done

# 驗:VM 上 probe 檔殘留情況
node dist/index.js exec --account <a> -e <e> \
  'import os,glob; print([f for f in glob.glob("/content/.colab-transfer-tmp/__uplink_probe__*")])'
# 預期: 0~1 個最新 probe 檔(不再增生),不會每次上傳多一個
```

---

## Round 2 回歸（全部都要跑）

| ID | 說明 |
|---|---|
| T1–T10 | **全部重跑**——尤其 T5(skip setup)、T8(cancel) 是這輪動到的核心 |
| T11–T16 | 新增，見上 |

## 收尾額外檢查（新增）

```bash
# 沒有任何 VM 殘留?
node dist/index.js cluster status
# 造成的 runtime 全 destroy
node dist/index.js runtime list --account <每個帳號> | grep -v "No runtimes"

# probe 檔不會在 VM 上堆?
# (T16 已驗)
```

<!-- APPEND-ANCHOR -->


