# Kafka Listener 轻量级框架设计方案

版本：v1.2（实现对齐）
日期：2026-10-28
性质：实现设计。已提供对应 Java 17 / Kafka Client 4.2.0 实现，实际 API 与完整使用示例以 README 和源码为准。

本版调整：手动异步提交；暂停以当前完整批次为边界；暂停后保留维护性 poll，不获取下一批业务消息。

**1. 目标与确定的需求**

构建一个独立 Java 消费框架，不依赖 Spring/Spring Boot。业务方声明 Listener、显式注册对象，自行决定启动、暂停与恢复时机。框架负责 Kafka 生命周期、订阅、拉取、提交和异常恢复。

| 项目 | 规则 |
|---|---|
| 消息源 | 仅 Kafka Consumer |
| 订阅方式 | subscribe(topics, rebalanceListener)，由 Kafka 分配分区 |
| 提交方式 | 关闭自动提交，由框架调用 commitAsync(offsets, callback) 手动异步提交 |
| 初始化 | 注册只解析定义和校验，不创建 Consumer，不连接 Kafka |
| 启动 | 业务主动调用 start；可用实例复用，不存在或失效才创建 |
| 处理模式 | 每个 Listener 一个 Consumer、一个串行业务执行器，逐条处理 |
| 基础设施异常 | 记录日志，无限次退避恢复，30 秒起，最大间隔 30 分钟 |
| 业务异常 | 记录日志、跳过当前消息、允许提交，继续后续消息 |
| 暂停 | 不再准入新批次；当前已准入的完整批次继续处理并异步提交 |
| 多方暂停 | 所有暂停方解除后，才允许恢复 |
| 控制范围 | 当前管理器已注册 Listener；不提供跨进程协调 |
| 关闭 | 显式 close，停止重试和派发，尽力提交并释放资源 |

首版不实现自动包扫描、依赖注入、批量业务回调、并行分区处理、失败消息重试、死信队列、动态修改订阅或分布式启停。业务方法执行完成以同步返回为准，不支持返回后仍在后台处理的异步 Listener。

**2. 业务接入与 API**

使用“注解声明 + 显式对象注册”。自定义注解避免与其他框架混淆：

~~~java
public class OrderListener {

    @KafkaMessageListener(
        name = "orders",
        topics = {"order-events"},
        groupId = "order-service"
    )
    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        // 解析消息并同步执行业务
    }
}
~~~

下面按能力列出方法签名；实际入口为实现 AutoCloseable 的 final 类 ListenerManager：

~~~java
public interface ListenerManagerApi extends AutoCloseable {

    void register(Object listener);

    void start(String name);
    void startAll();

    void pause(String name, String owner);
    void resume(String name, String owner);

    void pauseAll(String owner);
    void resumeAll(String owner);

    ListenerSnapshot status(String name);

    void close();
}
~~~

业务自行创建对象并注入依赖，框架只检查已注册对象上的注解方法。注册方法需为非 static、public、返回 void，参数为一个 ConsumerRecord<byte[], byte[]>。同一对象可有多个监听方法，但 name 在管理器内唯一。

~~~java
ListenerManager manager = ListenerManager.create(kafkaProperties);
manager.register(new OrderListener());

// 业务条件具备后，显式启动
manager.start("orders");

manager.pause("orders", "dataMigration");
manager.pause("orders", "configReload");

try {
    // 执行数据迁移
} finally {
    manager.resume("orders", "dataMigration");
}

// configReload 仍存在，不能恢复消费
manager.resume("orders", "configReload");

// 应用退出时关闭；构建管理器和注册均不自动消费
manager.close();
~~~

register 同步校验定义。重复 name、非法签名、空 topics/groupId/owner、未知 name、关闭后调用等编程错误直接报错。Kafka 运行异常不从 start 向调用方传播，按日志和后台重试处理。

首版要求先注册、后调用 start/startAll；首次启动后冻结注册表，简化全量操作的边界。

**3. 启动意图、多方暂停与状态**

每个 Listener 维护 startedRequested；每个管理器维护 globalPauseOwners；每个 Listener 维护 localPauseOwners。另有管理器/容器的 closing、closed 标记。

~~~java
allowedToStartBatch =
    startedRequested
    && globalPauseOwners.isEmpty()
    && localPauseOwners.isEmpty()
    && !closing
    && !closed;
~~~

新批次准入还要求实例可用、已订阅、分区归属有效、上一批提交已确认且业务执行器空闲。暂停控制门只约束下一批次，不中断已准入批次内剩余消息；关闭期限、实例失效和分区归属失效仍可终止相关派发。

start 仅将 startedRequested 设为 true，并唤醒协调循环。它不删除任何暂停记录，不重置重试期限，也不创建第二套任务。

| 情况 | start 后的结果 |
|---|---|
| 无实例，无暂停 | 创建、订阅、poll 并派发 |
| 可用实例已暂停，且无暂停方 | 复用实例，恢复 |
| 运行中 | 幂等返回 |
| 有任何暂停方 | 记录启动意图，不准入新批次；当前批次继续；尚无实例时暂不创建 |
| 正在创建或恢复 | 复用现有任务和退避期限 |
| 实例存在但尚未订阅 | 在允许运行后先完成订阅，不能直接 poll |
| 实例失效 | 允许运行且到达重试期限后，退休旧实例并重建 |
| 已关闭 | 报错 |

pause(name, owner) 在本地集合中加入 owner；resume(name, owner) 仅移除本地 owner。相同 owner 的重复 pause 幂等，重复 resume 幂等，不允许释放他人的暂停。

pauseAll(owner) 加入管理器级集合；resumeAll(owner) 仅删除管理器级记录。全量暂停和单个 Listener 的暂停处于两个独立作用域，即使字符串相同也互不抵消：

| 操作 | 全局暂停方 | orders 本地暂停方 | orders 是否允许新批次 |
|---|---|---|---|
| pauseAll("maintenance") | maintenance | 空 | 否 |
| pause("orders", "migration") | maintenance | migration | 否 |
| resumeAll("maintenance") | 空 | migration | 否 |
| resume("orders", "migration") | 空 | 空 | 已请求启动时允许 |

最后一条有效暂停记录解除后，已经请求启动的 Listener 自动恢复；从未请求启动的 Listener 保持未启动。

固定 owner 表示一份流程状态，不做重入计数。如果同一流程可能有多个独立并发执行，每次执行使用唯一 owner/token，例如 migration:<executionId>。各执行在 finally 中释放自己的 token。不自动按超时释放暂停，避免业务尚未结束就恢复消费。

API 为非阻塞控制请求。pause 返回不表示当前批次已结束或已提交成功。批次准入和暂停记录更新使用同一短锁，使两者具有确定顺序：先准入的批次允许完整执行；先登记暂停则不准入该批次。不能持有控制锁执行业务代码。

“当前批次”指一次非空 poll 的结果已被框架接纳并登记 batchId。若暂停与正在执行的 poll 竞争，返回记录只有在控制门仍开放时才能准入；否则作为最多一批的待准入缓存保留，解除暂停后再执行。维护性 poll 本身不产生新的业务批次。即使处理第 3 条时暂停，第 4 条到本批最后一条也可继续执行。

建议运行状态为 NEW、STARTING、RUNNING、PAUSING、PAUSED、RETRYING、CLOSING、CLOSED、FAILED。快照同时提供 startedRequested、暂停方、assignedPartitions、batchId、批次完成条数、commitState、最近失败阶段、nextRetryAt。RUNNING 不保证一定有分区或消息；PAUSING 表示当前批次仍在处理或等待提交确认；PAUSED 表示当前有效批次已收尾，不再有待确认的有效提交。失败时可显示 RETRYING，同时通过暂停集合保留业务暂停意图。归属已失效的未确认进度不冒认成功，后续允许重放。

**4. 内部结构与线程模型**

| 组件 | 职责 |
|---|---|
| ListenerManager | 注册表、全局暂停记录、控制入口、资源关闭 |
| ListenerDefinition | 不可变监听定义、目标对象、方法和 Kafka 配置 |
| ListenerContainer | 单个 Listener 的运行状态、控制门、线程和 Consumer |
| OffsetTracker | 当前分区归属下的已完成进度和待提交进度 |
| RetryPolicy | 连续失败次数、退避间隔和重试期限 |
| ConsumerFactory | 构造 Consumer；便于注入测试替身 |

每个 Listener 有一个消费线程和一个串行业务线程。

消费线程独占 Consumer 的 create/subscribe/poll/pause/resume/commit/close，业务线程不访问 Consumer。外部线程只更新控制状态、发送唤醒信号；必要时使用线程安全的 consumer.wakeup() 打断 poll。

业务线程一次只执行一条消息，结束后发送完成结果给消费线程。消息和完成结果包含 consumerInstanceId、partitionOwnershipEpoch、topic、partition、offset。

最多保留一批 poll 返回的记录，外加一条正在执行的记录（该记录属于本批）。仍有缓存、在途消息或待确认提交时，暂停已分配分区的进一步拉取，但继续维护性 poll。整批处理完成、异步提交回调确认成功且新批次控制门开放后，才恢复拉取。这个保守策略使每个 Consumer 最多一个有效提交请求在途，优先保持状态和恢复逻辑简单；消费线程本身不阻塞等待回调。

新分区分配时也要按当前控制门、缓存、在途任务和提交恢复状态决定是否立即 pause，不能使缓存无界增长。max.poll.records 限制每批条数，字节内存还需结合 fetch 大小和消息大小配置控制。

双线程的目的是让耗时业务不阻塞维护 poll，同时保持单 Listener 串行。Kafka Consumer 不是线程安全对象；max.poll.interval.ms 也不是仅靠心跳就能满足的约束。相关依据见文末官方文档。

**5. 消费、解码与提交**

强制框架拥有以下配置，不允许外部配置重新启用自动提交或替换底层字节反序列化器：

~~~properties
enable.auto.commit=false
key.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer
value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer
~~~

bootstrap.servers、group.id、认证信息等由业务配置。auto.offset.reset 需显式配置，不在设计中默认为 earliest 或 latest，以免引入意外回放或跳过。

底层读取 byte[]。JSON 等业务解析放在受保护的 Listener 调用中；解析失败与其他业务异常一样日志记录并跳过，避免坏数据在 Kafka 反序列化阶段反复阻塞 poll。若未来增加类型转换器，也必须放在同一个异常保护边界内。

执行规则：
1. 消费线程取得一批记录，在短锁内检查新批次控制门并登记 batchId，随后暂停进一步拉取。
2. 业务线程逐条执行本批消息，每条执行前检查归属有效性和关闭期限；业务暂停记录不打断本批剩余消息。
3. 正常返回，记录 SUCCESS；普通业务异常，记录日志并返回 SKIPPED。
4. 消费线程校验结果属于当前 Consumer 实例和该分区的有效归属，再推进可提交进度。
5. 继续完成本批所有仍有有效归属的消息；暂停只阻止新批次进入。
6. 本批全部完成后异步提交明确的分区进度；回调确认成功且新批次控制门开放后，才开始下一批。

每个分区仅推进已经处理或明确跳过的前缀，不要求 offset 数字连续；Kafka offset 本来可能存在间隙。简单安全的提交位置取该分区最后一个已处理记录的 offset + 1。

~~~java
Map<TopicPartition, OffsetAndMetadata> offsets = ...;
// 由消费线程调用；onCommitResult 只登记结果，不阻塞、不递归重试
consumer.commitAsync(offsets, this::onCommitResult);
~~~

调用返回只表示请求已发起，不能将其当作已提交成功。正常运行、暂停收尾、重平衡收尾和关闭都不切换为同步提交。使用带明确 offsets 的接口，避免误提交维护性 poll 或未准入缓存的位置；无新进度时不提交。

示例：同一分区本批 offset 100～109，处理 102 时暂停，继续处理 103～109。若期间没有归属变化，整批正常结束或业务异常跳过后，异步提交 110；回调成功后完成暂停。不会再准入下一批。暂停本身不再触发单条消息边界的部分提交。

异步提交协调器维护 batchId、commitRequestId、不可变 offsets 快照、Consumer 实例标识及各分区归属版本，状态为 PENDING、IN_FLIGHT、CONFIRMED 或 RETRY_WAIT。每个 Consumer 最多一个有效请求在途，避免旧 offset 的延迟重试覆盖较新进度。回调先校验请求、实例和归属，再记录成功或失败；不在回调内 sleep、阻塞或直接重新发起提交。过期回调只做必要日志，不改变当前进度。

提交失败时：
- 保留仍属当前有效归属的待提交进度，不标记提交成功。
- 不准入下一批，按退避规则恢复提交，同时维护 poll；当前批次此前已经处理完，不重新执行业务。
- 不因为提交失败就立即重复执行当前内存中已完成的业务。
- 分区归属发生变化后，丢弃不再有效的待提交项；由 Kafka 已提交位置恢复。
- Consumer 必须重建时，不能把旧实例缓存和旧进度直接迁入新实例。

commitAsync 发起时也可能同步抛出异常，需进入同一恢复流程。提交回调失败后，只有消费协调循环能在到期且归属仍有效时再次提交；不能凭调用返回或等待超时就假定请求已经结束并叠加重试。若回调长时间无法完成且实例已不健康，先退休实例，再按新实例恢复，旧回调始终失效。

本方案允许“处理完成但尚未提交”时的重复消息；不能保证业务恰好执行一次。对于业务抛错的消息，采用业务方已确认的日志跳过策略，不保证业务处理成功，也不自动重试失败消息。

**6. 暂停和恢复**

暂停是停止下一批业务消息准入及进一步拉取，当前已准入批次允许完整处理和异步提交。保留可用 Consumer 和分区订阅，继续维护性 poll。它不关闭 Consumer，也不暂停整个 topic 或其他应用实例。

收到暂停请求后：
1. 登记暂停方，立即关闭新批次准入门；消费线程对当前 assignment 调用 pause。
2. 当前已准入批次的全部有效消息继续逐条处理，普通异常仍按跳过处理。
3. 消费线程继续维护性 poll，处理成员维护、重平衡与异步提交回调，不恢复分区拉取。
4. 整批结束后，使用明确 offsets 发起 commitAsync。
5. 回调成功后，清理本批并进入 PAUSED；回调失败则保持禁止新批次并按退避规则重试提交。
6. 如果暂停时没有已准入批次和有效待提交项，可直接进入 PAUSED。暂停与 poll 竞争产生的待准入缓存保留到恢复时处理。

维护性 poll 的间隔必须满足 max.poll.interval.ms，而不是跟随最长 30 分钟的故障退避间隔。正常情况下，暂停全部已分配分区后 poll 不返回这些分区的新记录。发生重新分配时，分配回调必须对新分区重新应用 pause。

最后一个暂停记录解除后，检查 startedRequested、关闭状态、重试期限、实例健康、本批是否结束及异步提交是否已确认。满足条件才允许下一批。若当前批次尚未结束，则让它按原流程完成；不能提前开启第二批。仅重用已存在对象还不够，还必须检查订阅是否完成、实例是否可用。

如果暂停到达时 create/subscribe 正在执行，不能保证瞬时取消同步调用；调用返回后重新检查控制门。已创建实例可保留，不再推进非必要启动步骤或派发业务。实例失效且处于暂停时，不主动新建恢复，等允许运行后再处理。

**7. 异常分类和无限退避**

基础设施异常从 30 秒开始，指数增长，最大 30 分钟：
30 秒、1 分钟、2 分钟、4 分钟、8 分钟、16 分钟、30 分钟，之后每次 30 分钟。

每个容器维护恢复周期及失败次数。一次“创建成功”或“订阅调用返回”不重置计数；完成正常消费维护周期且没有未解决的提交失败后，重置。新一轮故障再从 30 秒开始。

| 场景 | 处理 |
|---|---|
| 创建失败 | 记录阶段、错误、下次重试时间，等待后重新创建 |
| 订阅阶段失败 | 退休并尽力关闭不完整实例，退避后重新创建并订阅 |
| 可恢复 poll 异常 | 保留可用实例，退避后再次尝试，不做无意义重订阅 |
| Consumer 已不可继续使用 | 停止使用旧实例，尽力关闭，退避后重建 |
| commitAsync 发起失败或回调失败 | 保存有效待提交项，禁止下一批，维护 poll，按期限重试 |
| 分区失去归属导致提交失败 | 进入归属清理流程，不能无限提交旧分区 |
| 认证、权限等运行失败 | 按要求持续日志与退避；无人工修复时可能一直不恢复 |
| 普通业务异常或业务解码异常 | 记录 topic/partition/offset 和异常，跳过，继续 |
| 控制触发的 WakeupException | 消费控制信号，不计作基础设施失败 |
| JVM 致命 Error | 不作为可吞掉的普通业务异常，记录 FAILED 并进入终止清理 |

实现需按选定 Kafka Client 版本明确异常分类，不能用 catch 后对全部异常一律重新创建。即使 Kafka Client 内部已自动重试，框架也要接住最终逃逸的异常。

退避保存 nextRetryAt/单调时钟期限，不做无法唤醒的长时间 sleep。维护 poll、控制处理和到期恢复动作分开调度。Kafka 网络请求配置有界超时；异步提交不使用同步等待或轮询阻塞业务线程。异步提交回调必须短小、无阻塞，具体请求超时配置按选定 Kafka Client 版本确定。

有有效实例时，可在内部暂停拉取后维护 poll；若持续 poll 故障使成员资格丢失，按重建和重新分配恢复，不承诺故障期间保留原分区。

重复 start/resume 不跳过现有重试期限，防止频繁控制导致重试风暴。pause/close 必须能唤醒等待；close 终止重试。暂停期间不新建恢复消费者，但已存在实例的必要维护和有效进度提交可以继续。

**8. subscribe 模式下的分区重新分配**

注册 ConsumerRebalanceListener，并维护每个分区的归属版本。Consumer 重建也分配新的实例标识。

onPartitionsRevoked：
- 停止派发被撤销分区的消息。
- 吸收已完成的有效结果，若没有在途提交，尽力对被撤销分区已完成的进度发起 commitAsync；若已有在途请求，不额外叠加请求。
- 不等待尚未完成的业务方法，更不能在回调里无限重试。
- 失效该分区归属版本，清除它尚未派发的缓存和未决提交。
- 当前在途业务可正常结束，但其迟到结果不能再推进该分区的新归属进度。

严格采用异步提交时，撤销回调内不等待提交成功，也不以 commitSync 兜底。因此撤销时不能保证最终进度已落盘，未确认部分允许重新消费。此场景下可以对已完成的部分进度尽力异步提交；“完整批次收尾”适用于普通业务暂停，不能覆盖分区被撤销、实例失效或关闭期限到达等故障边界。

onPartitionsLost：
- 分区可能已由其他成员持有，不再提交其 offset。
- 直接失效归属、清缓存和未决进度。

onPartitionsAssigned：
- 对新获得分区建立归属记录；保留的有效分区不能误清缓存或提交进度。
- 按全局/本地暂停、提交恢复、已有缓存和在途状态重新应用 pause。
- 不能因为新分区到达就绕过业务暂停。

Kafka 不会在 rebalance 后自动保留 pause/resume 状态。旧在途业务也可能与其他实例的新处理发生重叠，框架不能撤回已经产生的业务副作用，仍需业务幂等。

所有提交和业务完成结果都必须同时校验“Consumer 实例 + 分区归属版本”。即使同一分区稍后又分配回当前容器，旧回执也不能被当作本轮结果。

**9. 关闭、日志与边界**

close 是终态，不等同 pause。它关闭新批次准入、终止长期重试，在关闭预算内尽力完成当前有效批次，发起异步提交并继续维护性 poll 等待回调，然后由消费线程关闭 Consumer，结束执行器。全程不切换为 commitSync。若预算耗尽，则停止剩余派发并关闭，不冒认未确认提交成功；未确认进度以后允许重放。

默认优雅等待时间建议 30 秒并允许配置。超时后不再接受迟到结果、不再派发业务，退休 Consumer 并完成有界关闭流程；不能安全强杀任意 Java 业务代码。永久阻塞的业务线程可能不能及时退出，必须记录日志并由应用层处理。框架不对业务永久阻塞、进程被杀、JVM 致命错误承诺正常结束。

start 立即返回，只表示接受控制意图；subscribe 返回不表示已获得分区，commitAsync 返回不表示已提交成功。快照应明确区分启动中、运行但无分区、当前批次收尾中、异步提交在途、提交恢复和已暂停。

运行日志至少包含：
- Listener name、group.id、阶段（CREATE/SUBSCRIBE/POLL/COMMIT/HANDLE/CLOSE）。
- topic、partition、offset（有记录时）。
- 异常类型与堆栈、连续失败次数、下次重试时间。
- 控制状态变更及暂停方；恢复成功日志。
- 关闭超时、迟到结果被丢弃等重要情况。

不默认打印完整消息内容或认证配置。业务失败即使当前消息已跳过，进度提交失败后仍可能再次收到该消息，这不是框架主动业务重试。

**10. 验收场景**

| 编号 | 场景 | 预期 |
|---|---|---|
| 1 | 注册后不 start | 无 Consumer、无 Kafka 连接 |
| 2 | 首次 start | 创建、subscribe、poll，正常派发 |
| 3 | 并发重复 start | 一套容器任务，一个活跃 Consumer |
| 4 | 无实例先 pause，再 start | 保存启动意图，不创建；最后解除后创建 |
| 5 | 创建中 pause | 同步调用结束后不派发业务 |
| 6 | 已有实例暂停再恢复 | 复用实例，不重复订阅 |
| 7 | A、B 都暂停，A 解除 | B 仍阻止恢复 |
| 8 | 同 owner 重复 pause/resume | 幂等，不出现负数或多余暂停 |
| 9 | 全局与本地暂停叠加 | 各自解除，不互相抵消 |
| 10 | 同流程不同 token | 每个 token 均释放后才恢复 |
| 11 | start/resume 时仍有暂停方 | 不准入下一批；已准入批次仍可完整结束 |
| 12 | 批次第三条执行中暂停 | 继续完整批次，整批异步提交确认后暂停，不再准入下一批 |
| 13 | 业务异常/坏 JSON | 日志跳过，允许推进，继续下一条 |
| 14 | 创建或订阅持续失败 | 无限退避，最大 30 分钟 |
| 15 | 重试期间暂停/关闭 | 及时响应；不擅自恢复或重建 |
| 16 | 异步提交发起/回调失败 | 不冒认成功，不开启下一批，保存有效进度恢复 |
| 17 | 业务方法耗时较长 | 消费线程继续维护 poll |
| 18 | 暂停时 rebalance | 新分配分区重新暂停 |
| 19 | 撤销/丢失分区 | 清理该归属缓存；丢失后不再提交 |
| 20 | 旧在途结果迟到 | 不推进新归属或新 Consumer 的进度 |
| 21 | 处理后、提交前进程退出 | 可重复消费，不提前跳过未处理记录 |
| 22 | 消费者重建 | 从有效已提交位置恢复，不沿用旧缓存位置 |
| 23 | 运行但没有分区/消息 | 状态正常，不当作故障 |
| 24 | close 以及 close 后 start | 有界清理，后续 start 报错 |
| 25 | commitAsync 调用返回但回调未到 | 仍为提交在途，不标记已提交或开启下一批 |
| 26 | 暂停后异步提交回调到达 | 维护性 poll 使回调正常执行，成功后完成暂停 |
| 27 | 旧请求或旧实例回调迟到 | 不回退当前提交进度，不递归提交旧 offset |
| 28 | 当前批次处理中解除最后暂停 | 本批仍按序处理并确认提交，之后才开启下一批 |
| 29 | 本批处理中撤销分区 | 停止该分区后续派发，尽力异步提交有效前缀，允许重放 |
| 30 | pause 与数据 poll 返回竞争 | 按准入顺序处理，未准入批次暂存，不误处理或提交 |

纯控制状态、多方暂停、退避计算、offset 推进及迟到回执使用单元测试；subscribe、手动提交、暂停维持、重平衡和提交失败恢复使用真实 Kafka 集成测试。测试重点是行为和故障路径，不只验证内部方法被调用。

**11. 实现组织与扩展次序**

1. 实现注解、显式注册、定义校验及 ConsumerFactory。
2. 实现 startedRequested、全局/本地暂停集合、控制门与快照。
3. 实现双线程消费、单批缓存、普通业务异常隔离。
4. 实现明确 offset 的异步批次提交、单请求在途、提交回调与失败恢复。
5. 实现分区归属版本、重平衡回调、迟到结果隔离。
6. 实现无限退避、实例退休重建、提交恢复与关闭。
7. 执行上述验收场景，完成 API 说明及使用样例。

**12. 官方依据**

以下文档用于核对 Kafka 约束，未把文档版本指定为项目依赖版本；具体 Kafka Client/JDK 版本在实现时按现有项目环境固定。

- KafkaConsumer：线程安全、subscribe/poll、手动提交、暂停及维护 poll：
  https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
- ConsumerRebalanceListener：正常撤销、丢失分区和分配回调：
  https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.html
- Consumer 配置：max.poll.interval.ms、max.poll.records：
  https://kafka.apache.org/42/configuration/consumer-configs/
- RecordDeserializationException：
  https://kafka.apache.org/42/javadoc/org/apache/kafka/common/errors/RecordDeserializationException.html


**13. 当前实现补充**

实际入口为 ListenerManager.create(Map)，也提供配置 ListenerOptions 和 ConsumerFactory 的构造器。可显式注册注解对象，或 register(name, topics, groupId, MessageListener)。每个 Consumer 同时最多一个异步提交请求；watchdog 默认 2 分钟，超时退休实例并恢复，不能在旧请求仍有效时叠加重试。poll 默认 100 毫秒，关闭预算默认 30 秒且由所有 Listener 共享。实例退休后触发的旧重平衡回调也会被隔离。

测试与提交前评审结果见同目录 REVIEW.md；真实 Kafka 测试运行 mvn verify -Pbroker-it。
