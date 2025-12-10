package ru.time.timelog.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.time.timelog.model.TimeLog;
import ru.time.timelog.repository.TimeLogRepository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Slf4j
@RequiredArgsConstructor
public class TimeLogService {

    private static final ZoneId ZONE_ID = ZoneId.systemDefault();
    private static final int MAX_QUEUE_SIZE = 100_000;
    private static final int BATCH_SIZE = 100;
    public static final int DB_TIMEOUT_MS = 300;

    private final AtomicBoolean isDbAvailable = new AtomicBoolean(true);
    private final BlockingQueue<TimeLog> pendingLogs = new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);
    private final ExecutorService dbWriterExecutor = Executors.newFixedThreadPool(3);

    private final TimeLogRepository timeLogRepository;

    @Scheduled(cron = "${schedule.db.record.cron}")
    public void record() {
        TimeLog timeLog = createTimeLog();
        if (!isDbAvailable.get()) {
            addToPendingQueue(timeLog);
        } else {
            CompletableFuture.runAsync(() -> {
                        try {
                            timeLogRepository.save(timeLog);
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    }, dbWriterExecutor)
                    .orTimeout(DB_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("failed to save time log", ex);
                            isDbAvailable.set(false);
                            addToPendingQueue(timeLog);
                        } else {
                            log.debug("time log saved: {}", timeLog.getId());
                        }
                    });
        }
    }

    @Scheduled(cron = "${schedule.db.pending.cron}")
    public void recordFailedFromPending() {
        if (isDbAvailable.get() && !pendingLogs.isEmpty()) {
            List<TimeLog> batch = new ArrayList<>(BATCH_SIZE);
            pendingLogs.drainTo(batch, BATCH_SIZE);
            CompletableFuture.supplyAsync(() -> {
                        try {
                            timeLogRepository.saveAll(batch);
                            return batch.size();
                        } catch (Exception e) {
                            throw new CompletionException("batch save failed", e);
                        }
                    }, dbWriterExecutor)
                    .orTimeout(DB_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS)
                    .whenComplete((count, ex) -> {
                        if (ex != null) {
                            log.error("failed to save pending records batch", ex);
                            isDbAvailable.set(false);
                            batch.forEach(this::addToPendingQueue);
                        } else {
                            log.info("successfully saved {} pending records", count);
                        }
                    });
        }
    }

    @Scheduled(cron = "${schedule.db.check.cron}")
    public void restoreConnection() {
        if (!isDbAvailable.get()) {
            CompletableFuture.supplyAsync(() -> {
                        try {
                            timeLogRepository.checkConnection();
                            return true;
                        } catch (Exception e) {
                            throw new CompletionException("db is unavailable", e);
                        }
                    }, dbWriterExecutor)
                    .orTimeout(DB_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .whenComplete((success, ex) -> {
                        if (ex == null && success) {
                            isDbAvailable.set(true);
                            log.info("db is available, connection restored");
                        } else {
                            log.warn("db is unavailable", ex);
                        }
                    });
        }
    }

    public List<LocalDateTime> getLogs() {
        if (isDbAvailable.get()) {
            List<TimeLog> logs = timeLogRepository.findAll();
            List<LocalDateTime> result = new ArrayList<>();
            logs.forEach(timeLog -> result.add(toLocalDateTime(timeLog)));
            return result;
        } else {
            log.info("db is unavailable, try to get logs later");
            return Collections.emptyList();
        }
    }

    private LocalDateTime toLocalDateTime(TimeLog timeLog) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timeLog.getId()), ZONE_ID);
    }

    private void addToPendingQueue(TimeLog timeLog) {
        if (!pendingLogs.offer(timeLog)) {
            log.warn("queue is full, dropping time log: {}", timeLog.getId());
        }
    }

    private TimeLog createTimeLog() {
        TimeLog timeLog = new TimeLog();
        timeLog.setId(System.currentTimeMillis());
        return timeLog;
    }

}
