# Kafka Consumer Listener

轻量级 Java Kafka 消费框架。业务方决定什么时候启动、暂停、恢复；框架管理 Consumer、subscribe、维护性 poll、异步提交和异常恢复。无需 Spring 或 Spring Boot。

运行环境：JDK 17+、Maven 3.9+；本项目固定 Kafka Client 4.2.0。当前版本为源码开发版，尚未发布到 Maven Central。

## 快速接入

业务方自行创建对象和注入依赖，框架只检查显式注册的对象：

```java
import io.github.hippop.kafka.KafkaMessageListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Orders {
    @KafkaMessageListener(name = "orders", topics = "order-events", groupId = "order-service")
    public void handle(ConsumerRecord<byte[], byte[]> record) {
        // 同步完成解析和业务处理；普通异常会被记录并跳过。
    }
}
```

```java
import io.github.hippop.kafka.ListenerManager;
import java.util.Map;

ListenerManager manager = ListenerManager.create(Map.of(
    "bootstrap.servers", "localhost:9092",
    "auto.offset.reset", "earliest"
));
manager.register(new Orders());  // 不创建 Consumer、不连接 Kafka、不启动线程

// 由业务决定时机；例如依赖就绪、数据迁移完成之后。
manager.start("orders");

manager.pause("orders", "migration");
manager.pause("orders", "reload");
manager.resume("orders", "migration");  // reload 仍持有暂停，不能恢复
manager.resume("orders", "reload");     // 最后一个暂停释放后自动恢复

manager.close(); // 应用退出时调用
```

完整可编译示例见 [ConsumerExample.java](src/main/java/io/github/hippop/kafka/example/ConsumerExample.java)。也支持直接注册函数：

```java
manager.register("orders", java.util.List.of("order-events"), "order-service", record -> {
    // 同样遵循批次暂停、异常跳过及框架提交规则。
});
```

## 生命周期与多方暂停

| API | 行为 |
|---|---|
| register | 保存并校验定义；首次 start 后不再允许新增注册 |
| start(name) / startAll() | 请求启动；首次创建，可用实例复用；不会清空任何暂停记录 |
| pause(name, owner) / resume(name, owner) | 增加或移除指定 Listener 的本地暂停记录 |
| pauseAll(owner) / resumeAll(owner) | 增加或移除管理器的全局暂停记录 |
| status(name) | 返回不可变快照：控制意图、实际状态、批次、提交状态、分区、最近异常及重试时间 |
| close | 有界优雅关闭；关闭后不允许重新启动 |

start、pause、resume 提交请求后返回。start 返回不表示已经获得分区；pause 返回不表示当前批次已结束或提交已成功。status 的运行视图为最终一致，控制集合反映查询时的当前意图。

开启新批次必须同时满足：已请求启动、全局暂停集合为空、本地暂停集合为空、未关闭、实例可用、上一批异步提交已确认。

相同作用域下的相同 owner 重复 pause/resume 幂等。全局和本地记录互不抵消。同一流程有独立并发执行时，每次使用唯一 token（例如 migration:<executionId>），并在 finally 中释放。框架不按超时自动解除暂停。

所有控制都局限于本管理器，不影响其他进程或其他消费者组。尚未启动时可以登记暂停；start 只记录启动意图，等全部释放后再创建 Consumer。resume 本身不启动从未 start 的 Listener。

## 暂停按完整批次收尾

批次为一次非空 poll 返回、且通过控制门准入的记录集合。Listener 回调仍然一次一条、串行调用。

1. 登记暂停方，阻止下一批次准入。
2. 消费线程 pause 当前分配的分区，保留 Consumer 和订阅。
3. 已准入批次的全部有效消息继续执行；普通业务异常记录后跳过。
4. 整批结束后 commitAsync 明确的分区 offset。
5. 维护性 poll 继续运行，处理成员维护、重平衡和异步提交回调。
6. 回调成功后完成暂停；失败则持续禁止下一批，并退避恢复提交。

例如处理本批第 3/10 条时暂停，剩余 7 条继续执行，最终提交整批进度。暂停与数据 poll 返回竞争时，尚未准入的最多一批记录会暂存到恢复后，不提前处理或提交。

仅暂停分区拉取，不能彻底停止 poll；max.poll.interval.ms 仍然需要满足。重平衡后重新对新分配分区应用暂停。

## 消费、异步提交与重试

每个 Listener 对应一个 Consumer 线程和一个串行业务线程。Consumer 的订阅、poll、pause/resume、提交和关闭都由 Consumer 线程执行；外部线程仅通过线程安全的 wakeup 发送唤醒。最多保存一批记录。

框架强制 enable.auto.commit=false，并使用 ByteArrayDeserializer 读取 key/value。业务解析放在异常保护范围内，坏 JSON 等数据不会让 poll 一直卡在同一条。业务方法必须同步返回；在回调中启动异步任务后立即返回，会导致框架提前认为处理完成。

每个 Consumer 最多一个有效异步提交请求在途。commitAsync 返回只是请求已发起，收到成功回调后才允许下一批；消费线程继续 poll，不阻塞等待。此策略优先保持实现简单，暂不做跨批次流水线。

| 故障 | 行为 |
|---|---|
| Consumer 构造失败 | 日志、退避后重新创建 |
| subscribe 失败 | 退休并关闭不完整实例，再创建订阅 |
| 可重试 poll 异常 | 保留可用实例，按期限重试；poll 本身持续失败时不能承诺维持成员资格 |
| 其他 poll 故障 | 退休实例，按期限重建 |
| 提交发起或回调失败 | 保存有效进度，禁止下一批，维护性 poll，退避后重新提交 |
| 提交回调超过 watchdog | 退休实例，按已提交进度恢复；旧回调不影响新实例 |
| 普通业务异常 | 日志跳过，允许提交，不停止当前批次 |
| Error | 停止该容器并报告 FAILED；不吞掉致命错误 |

默认无限重试：30 秒、1 分钟、2 分钟、4 分钟、8 分钟、16 分钟、30 分钟，此后每次 30 分钟。创建或订阅成功本身不重置计数，需回到无未决恢复的健康消费周期。

异步提交回调只登记结果；不在回调中休眠、递归重试或阻塞。控制操作不会跳过已设置的退避期限。暂停期间不主动重建实例，但已有实例的维护性 poll 和有效提交恢复继续。

## 分区归属、重复消费与关闭

业务结果和提交请求携带 Consumer 实例与分区归属版本。分区撤销后，停止该分区剩余消息；旧业务结果与旧提交回调不能推进新归属进度。

撤销分区时仅尽力异步提交已完成前缀，不等待、不使用同步提交兜底；丢失分区后不再提交。普通暂停允许整批收尾，分区撤销、实例失效或关闭超时仍可能中断该批次。

处理后、提交确认前发生故障可能重复消费，业务需要幂等。业务失败采用日志跳过策略，框架不保证该条业务最终成功。

close 使用所有 Listener 共享的等待预算，默认 30 秒。预算内尽力完成当前批次、异步提交并维护 poll，随后关闭资源。超时后停止剩余派发，不冒认提交成功，也不强杀业务线程。永久阻塞业务可能无法结束；框架线程为 daemon，需由应用管理业务自身的超时。

## 配置与验证

bootstrap.servers 和 auto.offset.reset 必须显式提供；group.id 来自 Listener 定义。Kafka 认证、网络请求超时等配置直接放入配置 Map。框架会覆盖自动提交与底层反序列化器配置。

```java
var options = new io.github.hippop.kafka.ListenerOptions(
    java.time.Duration.ofMillis(100), // 维护性 poll 超时
    java.time.Duration.ofSeconds(30), // 首次失败退避
    java.time.Duration.ofMinutes(30), // 最大退避
    java.time.Duration.ofMinutes(2),  // 提交回调 watchdog
    java.time.Duration.ofSeconds(30)  // 总关闭预算
);
var manager = new ListenerManager(config, options, org.apache.kafka.clients.consumer.KafkaConsumer::new);
```

poll 超时需为 max.poll.interval.ms 留出余量；提交 watchdog 应结合 Kafka 请求超时配置。运行日志包含名称、组、故障阶段、重试次数和时间；默认不打印消息正文。

```bash
mvn test                       # 可控故障与并发单元测试
mvn verify -Pbroker-it         # 包含真实 KRaft broker 的集成测试，无需 Docker
mvn install                    # 安装开发版到本地 Maven 仓库
```

broker-it 使用 Maven 的测试依赖启动独立 Kafka JVM，数据与诊断日志写入 target/broker-it/。GitHub Actions 执行同样的集成测试。

设计细节见 [DESIGN.md](docs/DESIGN.md)，提交前检查记录见 [REVIEW.md](docs/REVIEW.md)。
