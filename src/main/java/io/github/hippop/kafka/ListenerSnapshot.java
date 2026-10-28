package io.github.hippop.kafka;

import java.time.Instant;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;

/** An immutable, eventually consistent runtime view with current control intentions. */
public record ListenerSnapshot(String name, State state, boolean startedRequested,
                               Set<String> localPauseOwners, Set<String> globalPauseOwners,
                               Set<TopicPartition> assignedPartitions, long batchId,
                               int batchSize, int completedRecords, CommitState commitState,
                               long pollCount, String failurePhase, String lastError,
                               int consecutiveFailures, Instant nextRetryAt) {
    public ListenerSnapshot {
        localPauseOwners = Set.copyOf(localPauseOwners);
        globalPauseOwners = Set.copyOf(globalPauseOwners);
        assignedPartitions = Set.copyOf(assignedPartitions);
    }

    public enum State { NEW, STARTING, RUNNING, PAUSING, PAUSED, RETRYING, CLOSING, CLOSED, FAILED }
    public enum CommitState { NONE, PENDING, IN_FLIGHT, RETRY_WAIT }
}
