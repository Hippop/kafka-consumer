package io.github.hippop.kafka;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** Explicit lifecycle control. Registration never creates a consumer or starts a thread. */
public final class ListenerManager implements AutoCloseable {
    final Object controlLock = new Object();
    final Set<String> globalOwners = new HashSet<>();
    private final Map<String, ListenerContainer> containers = new LinkedHashMap<>();
    private final Map<String, Object> configuration;
    private final ConsumerFactory factory;
    private final ListenerOptions options;
    private boolean frozen;
    private boolean closed;

    public static ListenerManager create(Map<String, Object> configuration) {
        return new ListenerManager(configuration, ListenerOptions.defaults(), KafkaConsumer::new);
    }

    public ListenerManager(Map<String, Object> configuration, ListenerOptions options,
                           ConsumerFactory factory) {
        this.options = Objects.requireNonNull(options, "options");
        this.factory = Objects.requireNonNull(factory, "factory");
        Map<String, Object> copy = new HashMap<>(configuration);
        requireText(Objects.toString(copy.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), ""), "bootstrap.servers");
        requireText(Objects.toString(copy.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), ""), "auto.offset.reset");
        long maxPollMillis = Long.parseLong(Objects.toString(
                copy.getOrDefault(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000)));
        if (maxPollMillis <= options.pollTimeout().toMillis() * 3) {
            throw new IllegalArgumentException("pollTimeout must leave headroom below max.poll.interval.ms");
        }
        copy.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        copy.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        copy.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        this.configuration = Map.copyOf(copy);
    }

    public void register(Object listener) {
        Objects.requireNonNull(listener, "listener");
        synchronized (controlLock) {
            checkRegistration();
            List<Definition> definitions = new ArrayList<>();
            Set<String> signatures = new HashSet<>();
            for (Class<?> type = listener.getClass(); type != Object.class; type = type.getSuperclass()) {
                for (Method method : type.getDeclaredMethods()) {
                    if (method.isBridge() || method.isSynthetic()) continue;
                    String signature = method.getName() + Arrays.toString(method.getParameterTypes());
                    if (!signatures.add(signature)) continue;
                    KafkaMessageListener annotation = method.getAnnotation(KafkaMessageListener.class);
                    if (annotation == null) continue;
                    if (!Modifier.isPublic(method.getModifiers()) || Modifier.isStatic(method.getModifiers())
                            || method.getReturnType() != void.class || method.getParameterCount() != 1
                            || method.getParameterTypes()[0] != ConsumerRecord.class || !method.canAccess(listener)) {
                        throw new IllegalArgumentException("Listener must be accessible public void method(ConsumerRecord): " + method);
                    }
                    definitions.add(definition(annotation.name(), List.of(annotation.topics()),
                            annotation.groupId(), record -> invoke(listener, method, record)));
                }
            }
            if (definitions.isEmpty()) throw new IllegalArgumentException("No @KafkaMessageListener methods found");
            Set<String> names = new HashSet<>(containers.keySet());
            for (Definition definition : definitions) {
                if (!names.add(definition.name())) throw new IllegalArgumentException("Duplicate listener: " + definition.name());
            }
            definitions.forEach(this::add);
        }
    }

    /** Optional programmatic registration, sharing the same lifecycle as annotated listeners. */
    public void register(String name, List<String> topics, String groupId, MessageListener listener) {
        synchronized (controlLock) {
            checkRegistration();
            Definition definition = definition(name, topics, groupId, listener);
            if (containers.containsKey(name)) throw new IllegalArgumentException("Duplicate listener: " + name);
            add(definition);
        }
    }

    public void start(String name) {
        synchronized (controlLock) {
            checkOpen();
            ListenerContainer container = find(name);
            container.checkStartable();
            frozen = true;
            container.control.started = true;
            container.ensureStarted();
            container.signal();
        }
    }

    public void startAll() {
        synchronized (controlLock) {
            checkOpen();
            containers.values().forEach(ListenerContainer::checkStartable);
            frozen = true;
            for (ListenerContainer container : containers.values()) {
                container.control.started = true;
                container.ensureStarted();
                container.signal();
            }
        }
    }

    public void pause(String name, String owner) { updateLocal(name, owner, true); }
    public void resume(String name, String owner) { updateLocal(name, owner, false); }
    public void pauseAll(String owner) { updateGlobal(owner, true); }
    public void resumeAll(String owner) { updateGlobal(owner, false); }

    private void updateLocal(String name, String owner, boolean add) {
        requireText(owner, "owner");
        synchronized (controlLock) {
            checkOpen();
            ListenerContainer container = find(name);
            if (add) container.control.owners.add(owner);
            else container.control.owners.remove(owner);
            container.signal();
        }
    }

    private void updateGlobal(String owner, boolean add) {
        requireText(owner, "owner");
        synchronized (controlLock) {
            checkOpen();
            if (add) globalOwners.add(owner);
            else globalOwners.remove(owner);
            containers.values().forEach(ListenerContainer::signal);
        }
    }

    public ListenerSnapshot status(String name) {
        synchronized (controlLock) {
            return find(name).snapshot();
        }
    }

    @Override
    public void close() {
        List<ListenerContainer> copy;
        long deadline = System.nanoTime() + options.closeTimeout().toNanos();
        synchronized (controlLock) {
            if (closed) return;
            closed = true;
            copy = List.copyOf(containers.values());
            copy.forEach(container -> container.requestClose(deadline));
        }
        // One shared deadline, rather than a full timeout for every listener.
        for (ListenerContainer container : copy) container.awaitClose(deadline);
    }

    private void add(Definition definition) {
        Map<String, Object> config = new HashMap<>(configuration);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, definition.groupId());
        containers.put(definition.name(), new ListenerContainer(this, definition,
                Map.copyOf(config), factory, options));
    }

    private static Definition definition(String name, List<String> topics, String groupId, MessageListener listener) {
        requireText(name, "name");
        requireText(groupId, "groupId");
        Objects.requireNonNull(listener, "listener");
        List<String> copy = List.copyOf(topics);
        if (copy.isEmpty()) throw new IllegalArgumentException("topics cannot be empty");
        copy.forEach(topic -> requireText(topic, "topic"));
        return new Definition(name, copy, groupId, listener);
    }

    private ListenerContainer find(String name) {
        ListenerContainer result = containers.get(name);
        if (result == null) throw new IllegalArgumentException("Unknown listener: " + name);
        return result;
    }

    private void checkOpen() {
        if (closed) throw new IllegalStateException("Manager is closed");
    }

    private void checkRegistration() {
        checkOpen();
        if (frozen) throw new IllegalStateException("Register listeners before the first start");
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
    }

    private static void invoke(Object target, Method method, ConsumerRecord<byte[], byte[]> record) throws Exception {
        try {
            method.invoke(target, record);
        } catch (InvocationTargetException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof Exception ordinary) throw ordinary;
            if (cause instanceof Error error) throw error;
            throw new IllegalStateException(cause);
        }
    }

    record Definition(String name, List<String> topics, String groupId, MessageListener listener) {}

    final class Control {
        final Set<String> owners = new HashSet<>();
        boolean started;

        boolean allowed() {
            synchronized (controlLock) {
                return started && owners.isEmpty() && globalOwners.isEmpty() && !closed;
            }
        }
    }
}
