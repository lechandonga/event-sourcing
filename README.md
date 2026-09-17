# 本地事件溯源：事件存储 + 读模型同步

一套**只依赖本地（内存 / 本地文件）**、无任何外部服务的事件溯源（Event Sourcing）
与读模型（CQRS 投影）同步机制。提供乐观并发的只追加事件日志、按读取时升级的
事件结构演进、可在线重建的投影、检查点续传与幂等重放，以及带完整性校验的
快照与历史整理。

> 语言：Go（仅标准库，`go.mod` 无任何 require）。示例域为 `bank` 银行账户。

## 目录结构

| 包 | 职责 |
| --- | --- |
| `eventstore` | 只追加事件日志：内存实现与本地 JSONL 文件实现；流内版本 + 全局序号；乐观并发；订阅唤醒 |
| `event` | 事件类型注册表与 **upcaster 升级链**（读取时升级，永不改写历史） |
| `snapshot` | 聚合快照信封、CRC-32C 校验、内存/原子文件存储 |
| `projection` | 读模型投影仪：检查点、幂等有序重放、可定位可重试的失败记录、在线重建 |
| `repo` | 泛型聚合仓储：乐观并发保存、快照加速加载、损坏/过期/超前快照安全回退 |
| `bank` | 示例域：账户聚合、`AccountOpened/Deposited/Withdrawn` 的 v1→v3 演进、汇总读模型 |
| `internal/safeio` | 临时文件 + fsync + rename 的原子写 |
| `cmd/demo` | 端到端演示 |

## 快速开始

```bash
go test ./...                 # 单元/集成测试
go test -race ./...           # 含并发竞态检测（需要 CGO）
go run ./cmd/demo             # 内存模式
go run ./cmd/demo --mem=false # 本地文件模式（写 ./.demo-data）
```

## 1. 事件追加与乐观并发控制

每条聚合流（`StreamID{Type, ID}` → 流名 `"<type>-<id>"`）维护单调递增的
**流内版本**（从 1 起）；整个存储维护单调递增、无空洞的**全局序号**。

追加时传入 `ExpectedVersion`：

| 期望值 | 语义 |
| --- | --- |
| `ExpectedVersionNew` (0) | 流必须尚不存在（首次创建） |
| 正数 N | 流当前版本必须恰好等于 N |
| `ExpectedVersionAny` (-1) | 跳过校验（仅用于导入/迁移） |

- 校验失败时**整批拒绝**，没有任何事件进入已提交流（校验、分配版本、提交在
  内存实现中由同一把锁保护；文件实现为 prepare→fsync 落盘→复检提交，复检失败会
  把刚写入的行截断回滚）。
- 冲突返回 `*eventstore.ConflictError`，同时满足
  `errors.Is(err, eventstore.ErrVersionConflict)`，并可用区分性的 `Reason`：
  - `version-conflict`：并发竞争，版本落后（最常见，调用方应重新加载、重放命令、重试）；
  - `stream-exists`：期望新建但流已存在；
  - `stream-not-found`：声明了版本但流不存在；
  - `aggregate-type-mismatch`：同一流名的聚合类型身份不一致。

推荐的写入模式是 **CAS 重试**（见 `cmd/demo`）：

```
Load → 基于当前状态执行命令产生事件 → Save(expected=当前版本)
   └─ 冲突 → 丢弃草稿，重新 Load 后重试
```

## 2. 事件结构演进（兼容规则）

事件负载是带 `schemaVersion` 的 JSON。**已提交的历史事件永不被改写**；
读取时由 `event.Registry` 按版本号逐级执行纯函数 `Upcaster`，先升级到当前结构，
再解码成当前 Go 类型。升级只发生在读取副本上。

兼容规则（确定且稳定）：

1. **新增字段**：历史事件缺该字段时，由升级器填入**固定缺省值**。
   例：`currency` 缺省 `"USD"`；`note` 缺省 `""`。
2. **删除字段**：旧事件多出的字段在解码时被自然忽略，不报错。
3. **字段重命名 / 语义变更**：由升级器做**确定性转换**，不依赖任何外部状态。
   例：金额单位由“元”改为“分”，升级器执行 `cents = yuan * 100`。
4. **升级器必须逐级注册**（`1` 表示 v1→v2），不允许跳级；缺失升级链或遇到
   比注册表更新的版本会返回 `ErrBadSchemaVersion`，而不是猜测语义。
5. 省略 `schemaVersion` 的负载一律视为**当前版本**（新写入的事件由
   `event.Encode` 自动盖章）。
6. 解码使用 `json.Decoder.UseNumber`，大整数不会被 `float64` 污染。

`bank` 示例的演进：

```
AccountOpened
  v1 { holder, initialBalance }            # 金额单位：元
  v2 { holder, initialBalanceCents }       # 元 → 分（×100），字段重命名
  v3 { holder, initialBalanceCents, currency }  # 新增币种，缺省 USD
MoneyDeposited / MoneyWithdrawn
  v1 { amount } → v2 { amountCents }       # 元 → 分
                → v3 { amountCents, note } # 新增备注，缺省 ""
```

添加新版本的步骤：定义当前结构并提升 `EventSchemaVersion()`；为
`fromVersion = 当前-1 … 1` 补齐缺失的升级器；老版本代码从此不再需要出现。

## 3. 读模型重建

`Projector.Rebuild(ctx)` 在**不阻塞事件写入**的前提下从完整历史重建：

1. 先挂全局订阅，再读取当前末尾（杜绝通知缺口）；
2. `Reset()` 读模型、`Reset()` 检查点、清除当前失败记录；
3. 按全局序号分批从 0 重放全部历史；
4. 进入追赶循环，消费重建期间并发到达的写入，直到检查点追平存储末尾。

一致性保证：读模型的 `Apply` 被要求是**确定性纯状态转移**（不依赖时间、随机数、
外部状态）。因此“长期增量处理”与“一次性全量重放”消费的是同一串有序事件，
**结果必然相同**（测试 `TestRebuildMatchesIncremental` 逐字段断言）。

重建期间写入照常提交（订阅 + 轮询兜底，最终收敛），测试
`TestRebuildDoesNotBlockOrLoseWrites` 断言重建过程中的 100 个并发写入
零丢失、检查点最终等于存储最大序号。

> 重建期间读模型处于中间态；若需要查询不中断，可重建到影子实例再原子交换
> （`Reset/Apply` 的具体隔离方式由读模型自己决定）。

## 4. 检查点与幂等重放

- 检查点记录“已成功应用的最后一个事件的全局序号”，只有在事件应用成功后才前移
  （at-least-once）。逐事件应用、逐事件前移检查点，失败时精确停在问题事件之前。
- 普通保存是**单调**的：检查点回退返回 `ErrCheckpointRegression`，防止乱序/并发
  覆盖进度；只有 `Rebuild` 调用显式的 `Reset` 才能归零。
- 投影仪在每批上做三重防护，因此**重复、乱序、延迟到达**的事件都不会重复计数：
  丢弃序号 ≤ 检查点的事件 → 批内去重 → 按全局序号升序排序，再升级解码。
  订阅只是“有新事件”的唤醒信号（可能合并/重复），数据一律以 `ReadAll` 拉取为准。
- 失败处理：某个事件 `Apply` 失败时，
  - 检查点停住，后续事件**被阻塞而不是被跳过**（不会漏数）；
  - 写入一条可定位的失败记录（投影名、全局序号、流、流版本、事件类型、事件 ID、
    错误消息、首次/最近时间、尝试次数）；
  - `Run` 循环按指数退避（2×，上限 5s）重试；修复后下一次处理成功，失败记录清除。
  - 永远失败的“毒丸事件”可用 `CurrentFailure()` 精确定位（测试
    `TestPoisonEventBlocksButNeverSkipped`、`TestFailureRecordedAndRetried`）。

## 5. 快照与历史整理

快照信封携带**足以定位事件流位置**的信息：`StreamType/StreamID`、
快照所基于事件的流内 `Version` 与 `GlobalSequence`、聚合负载，以及
CRC-32C 校验和。加载后从 `Version+1` 继续顺序重放。

仓储加载时对任何快照异常都**安全回退到从版本 0 的完整重放，绝不跳过事件**：

| 情况 | 行为 |
| --- | --- |
| 快照缺失 | 直接全量重放（冷路径，正常） |
| 快照过期（落后于流） | 从 `Version+1` 重放剩余事件（常规加速路径） |
| 校验和不符 / JSON 损坏 / 负载无法恢复 | 删除坏快照 → 回调 `corrupt` → 全量重放 |
| 快照超前于流末尾 | 删除 → 回调 `ahead` → 全量重放 |
| 身份字段不匹配 | 删除 → 回调 `identity` → 全量重放 |
| 事件区间出现版本空洞 | 删除快照 → 回调 `gap` → 全量重放（并重检事件流） |

- 文件快照通过“临时文件 + fsync + rename + fsync 目录”原子覆盖，读者只会看到
  旧文件或完整新文件，不会读到半截快照。
- 快照按保存次数阈值自动重打（事件仍完整保留在日志里；快照只是重放加速，
  删除任意快照都不影响正确性——这就是“历史整理”的安全边界：可以删快照，
  不能删事件）。

## 本地持久化与崩溃语义

`eventstore.FileStore` 单文件布局：

```
ES-EVENTS-V1\n                         # 魔数文件头
[{"globalSequence":1, ...}, ...]\n     # 每个成功提交批次 = 一行 JSON 数组
```

- 每次追加：整批一行写入 → `fsync` → 发布内存视图。带有换行符的完整行一定对应
  一次成功提交。
- 崩溃在一行写到一半：启动时识别“末尾不带换行符的残余行”，**截断**后加载；
  已完整落盘的行逐事件校验全局序号与流版本的连续性，出现空洞/重复直接报
  `ErrCorrupt`（宁可拒绝启动，也不跳过事件）。
- 检查点、失败记录、快照均为独立本地文件，全部原子写入；失败历史是只追加的
  JSONL 审计文件，清除当前失败不会删除历史留痕。
- 进程重启后打开同一个文件即可恢复全部状态（测试 `TestRestartWithFileBackedState`）。

## 关键测试场景

```bash
go test ./eventstore/ -run TestConcurrentWritersSerializesByVersion -race  # 并发版本冲突
go test ./event/ -run TestUpcast                                         # 跨版本升级链
go test ./bank/ -run TestCrossVersionReplay                              # v1/v2/v3 混合重放
go test ./projection/ -run 'TestRebuild|TestIdempotent|TestFailure|TestPoison'
go test ./repo/ -run 'TestCorruptSnapshotFallback|TestAheadSnapshotFallback|TestSnapshotThenReplay'
go test ./eventstore/ -run TestFileStoreTruncatesPartialLine             # 崩溃半写恢复
```

## 设计边界与注意事项

- 事件是**不可变事实**：结构演进只允许“增加新理解”（升级读取视图），不允许修改
  已提交事实；金额单位这类语义变更必须显式、确定性地转换。
- 乐观并发的粒度是单个聚合流：同一聚合的写入串行化，不同聚合天然并行。
- 一个投影名在任一时刻只允许一个投影仪运行（`Run`/`Rebuild` 互斥）。
- 本实现面向单机本地场景；要扩展到多节点，需要替换 `eventstore.Store` /
  `CheckpointStore` 的实现并引入共享存储，但领域层与升级/投影语义无需改动。
