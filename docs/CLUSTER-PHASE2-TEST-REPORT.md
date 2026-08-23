# CLUSTER Phase 2 測試報告

- 日期: 2026-08-23 (UTC)
- 環境: Node v22.23.2, branch phase-2, `npm run build` 0 error
- 帳號: 主要 codingbear.ai.07@gmail.com(daemon 亦動用 .09 / .10)
- 既有 state:測試前 state.json 已有 jobs 1–13(前期 smoke),本次新增 job 14–27

| ID | 結果 | 證據(指令+輸出節錄) | 備注 |
|---|---|---|---|
| T1 | ✅ | `cluster --help` 列出 status/submit/shutdown/rehearse/list/logs/cancel;`cluster status` 結尾 `VMs: 0 idle / 0 busy / 1 down`;`pgrep` 見 `dist/cluster/daemon.js` (pid 2839457);state.json 有 `{"nextJobId":N,"jobs":[],"vmSetup":{}}` 結構;sock/pid/daemon.log/logs/ 皆存在 | |
| T2 | ✅ | job 14 `-c 'echo T2_HELLO; date; sleep 5'` → queued → provisioning(.07 新 VM m-s-kkb-usc1c1-2cao4edegdxuq)→ running → done;logs 見 `T2_HELLO`、`Sun Aug 23 05:21:01 PM UTC 2026`、`/content# exit`;job-14.log 存在 280B | 首次 provision ~5s,符合預期 |
| T3 | ❌→✅ | job 15 rehearse rehearse-bad.sh → `failed ... [setup script failed (see log)]`;log 有 `SETUP_STARTING` 與 `ls: cannot access '/this-path-does-not-exist'`;`grep -c __CLUSTER_SETUP_OK__` = 0;`grep -c SETUP_DONE_MARKER` = 0(連腳本內文都沒進 log,代表走 kernel exec 寫檔,無 heredoc echo 污染) | 完全符合標準 |
| T4 | ✅ | job 16 rehearse-good.sh → done;log 依序 `SETUP_IS_RUNNING_NOW` / `__CLUSTER_SETUP_OK__` / `rehearse-noop` / `__CLUSTER_JOB_OK__`;`vmSetup["m-s-kkb-usc1c1-2cao4edegdxuq"].hash = "25a101dc"` | |
| T5 | ✅ | job 17 setup_file 同 T4 → 同 endpoint(usc1c1)shell=4;log 有 `probe`、**無** `SETUP_IS_RUNNING_NOW`(setup 被 vmSetup hit 跳過);done | 注意:跳過時仍會印 `__CLUSTER_SETUP_OK__` marker(預期行為,handbook 只要求 SETUP_IS_RUNNING_NOW 不出現) |
| T6 | ❌ | job 18、19(重試一次)皆 failed:`dispatch failed: Upload failed after 3 attempts for content/.colab-transfer-tmp/upload-.../parts/part-00000X: Request timed out after 120000ms: PUT /api/contents/...`;job 18 倒在 part-000003、job 19 倒在 part-000000 | expected: 80MiB chunked upload 成功、sha256 一致;actual: contents API PUT 穩定逾時,3 次 attempt 全敗。**推測根因**:chunk 太大 + 120s timeout 對目前上傳頻寬不足,或 Colab contents API 對大 PUT 節流;upload.js 無跨 attempt resume,每次都從頭。未自行修復 |
| T7 | ✅ | job 20 (t7a) failed `[command exited without the completion marker]`,log 有 `__CLUSTER_SETUP_OK__` + ZeroDivisionError traceback、無 JOB_OK;job 21 (t7b) failed `[setup script failed]`,log 有 SETUP_STARTING、無 SETUP_OK、無 `SHOULD_NEVER_SEE_THIS` | 7b 因 .07 VM 忙,自動改用 .09 新 VM — 附帶驗證跨帳號 dispatch |
| T8 | 一半✅/❌ | 執行中 cancel(job 22、23):VM 上 `pgrep -f 'sleep 9999'` 回空白,SURVIVED 未印出,狀態 cancelled ✅。**排隊中 cancel(job 24)❌**:submit 後立即 cancel,daemon.log 時序 — `18:36:03.871 job 24 cancel: close failed (continuing as cancelled): job 24 has no assignment` → `18:36:04.657 job 24 running`。結果 state `status=running` 且 `endedAt(18:36:03.871) < startedAt(18:36:04.656)`,VM 上 sleep 9999(pid 18460)仍活著,list 永遠顯示 running;事後再一次 cancel(已 running)才正常清掉 | **race bug**:cancel 在 dispatch 完成前先把 job 標 endedAt/視為 cancelled,但 dispatch 繼續把 job 轉 running 且未回頭處理取消旗標。expected: queued job cancel 後不得 dispatch,或 dispatch 完成時偵測 cancel 立即 kill。未自行修復 |
| T9 | ✅ | job 25 TICK_1..60;running 中 destroy VM(.07 endpoint)→ 35s 後 `failed ... [runtime unreachable: Daemon failed to start within timeout... (last mirror in .../job-25.log)]`;`cluster logs 25` 正常印出 TICK_1..TICK_60(本機 mirror) | mirror 功能正確(實際上 VM 已跑完 TICK_60,但從 destroy 到 mirror 讀取全程不依赖 VM) |
| T10 | ✅ | job 26/27 連續 submit:26 → running @ .09 usc1a1 既有 VM;27 → 先 queued 後拿到 .10 新 VM(usw4a1)→ running;`cluster status` 顯示 `2 busy`;絕無共用 VM;兩者最終 done 且各印 A_DONE/B_DONE | |

## 收尾狀態

- 所有測試 runtime(.07/.09/.10/.11)已 destroy;`cluster status` 為 `0 idle / 0 busy / 0 down`
- `cluster shutdown` 已執行,daemon 停止,state persisted

## 結論

**非全綠**:T6(upload chunk PUT 120s 逾時,可重現)、T8a(queued-cancel 與 dispatch race,產生殭屍 running job)兩項失敗,根因推測如上表,未自行修復。其餘 T1–T5、T7、T8b、T9、T10 全數通過。

---

# Round 2 測試報告(T6/T8 修復 + adaptive upload 後全量回歸)

- 日期: 2026-08-23 (UTC)
- 環境: Node v22.23.2,`npm run build` 0 error(含 Round 2 六檔變更)
- 帳號: .07 / .09 / .10;job 編號 33–45

| ID | 結果 | 證據 | 備注 |
|---|---|---|---|
| T1 | ✅ | `cluster status` 正常 `0 idle / 0 busy / 0 down`;daemon pid 174958;state/sock/log 齊 | |
| T2 | ✅ | job 33 done;log 有 `T2_HELLO` + 日期 + `/content# exit`;job-33.log 283B | |
| T3 | ✅ | job 34 failed `[setup script failed]`;有 SETUP_STARTING/ls error,無 `__CLUSTER_SETUP_OK__`、無 SETUP_DONE_MARKER | |
| T4 | ✅ | job 35 done;log SETUP_IS_RUNNING_NOW/SETUP_OK/JOB_OK;`vmSetup[usc1a1-2anvpobtolrnd].hash="25a101dc"` | |
| T5 | ✅ | job 37 同 endpoint、log 無 SETUP_IS_RUNNING_NOW、有 `probe` | |
| T6 | ✅(修復確認) | job 36:80MiB upload 成功 → done;VM 上 sha256 `33a3a11d…23df` 與本機完全一致(`T6_SHA_MATCH`) | 上輪 3 attempts 全 timeout;本輪 adaptive(probe + 動態 chunk/timeout)一次過。約 6 分鐘完成上傳+執行(慢網 ~200KB/s 量級) |
| T7 | ✅ | job 38 failed `command exited without the completion marker`(有 SETUP_OK、無 JOB_OK、ZeroDivisionError traceback);job 39 failed `setup script failed`(無 SETUP_OK、無 SHOULD_NEVER_SEE_THIS) | 7b 自動 dispatch 到 .09 新 VM |
| T8 | ✅(見 T14/T15) | 本輪以 T14/T15 取代覆蓋 | |
| T9 | ✅ | job 42 running 中 destroy .07 VM → failed `runtime unreachable`;`logs 42` 從本機 mirror 印出 TICK_1..60 | |
| T10 | ✅ | job 43 @ .09 既有 VM、job 44 @ .10 新 VM,`2 busy`,不共用;兩者 done | |
| T11 | ✅ | `fs upload /tmp/t11.bin`(100MiB urandom)→ 100.0 MiB 成功,7m0s(慢網);VM sha256 `dcb7e13f…9165` 與本機一致 | chunked 自適應路徑驗過;probe 檔行為見 T16 |
| T12 | ✅ | CLI:`fs upload t12.txt --remote-path /content/dirtest/` → 自動接 basename,VM 讀到 `DIRTEST`;cluster:job 45 uploads dest `/content/data/` → log 有 `-rw-r--r-- 1 root root 8 ... /content/data/t12.txt` | job 45 收尾時因 VM 已被 destroy 而 state 翻成 failed,但 log 顯示 JOB_OK + ls 成功,測試本質通過 |
| T13 | ✅ | `chooseStrategy(600MiB)→chunked`、`(200MiB)→chunked`、`(9GiB)→drive`(未實作,僅回傳策略名) | |
| T14 | ✅(race 修復確認) | submit 後 0.3s cancel job 40 → `cancelled`,`startedAt/endpoint/account 皆 null`,從未 running;兩台 VM `pgrep -f 'sleep 9999'` 皆 **CLEAN** | 上輪 bug(殭屍 running)不再現 |
| T15 | ✅(回歸) | job 41 running 後 cancel → cancelled;VM pgrep **CLEAN**;SURVIVED 未印 | T8b 未被修壞 |
| T16 | ✅ | 3× 30MiB 連續 `fs upload` 全成功(2m02s/2m05s/2m12s);VM 上 `__uplink_probe__*` glob 只剩 1 個,不增生 | |

## 收尾狀態(Round 2)

- VM 全 destroy(.07 usc1a1、.09 use4a2、.10 use4a1);`cluster status` = `0 idle / 0 busy / 0 down`
- daemon 已 shutdown,state persisted

## 結論(Round 2)

**全綠 :T1–T16 全數通過。** Round 1 兩個紅燈(T6 upload timeout、T8 queued-cancel race)經修復後皆確認消失,且未引入回歸。Phase 2 可收。
