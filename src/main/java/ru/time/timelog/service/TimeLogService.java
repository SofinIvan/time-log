package ru.time.timelog.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.time.timelog.exception.DbException;
import ru.time.timelog.model.TimeLog;
import ru.time.timelog.repository.TimeLogRepository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Slf4j
@RequiredArgsConstructor
public class TimeLogService {

    private static final ZoneId ZONE_ID = ZoneId.systemDefault();
    private static final int MAX_QUEUE_SIZE = 100_000;
    public static final int DB_TIMEOUT_MS = 300;

    private final AtomicBoolean isDbAvailable = new AtomicBoolean(true);
    private final BlockingQueue<TimeLog> pendingLogs = new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);
    private final ExecutorService dbExecutor = Executors.newFixedThreadPool(2);

    private final TimeLogRepository timeLogRepository;

    @Scheduled(cron = "${schedule.db.record.cron}")
    public void record() {
        addToPendingQueue(createTimeLog());
        if (isDbAvailable.get()) {
            List<TimeLog> batch = new ArrayList<>();
            CompletableFuture.supplyAsync(() -> {
                        try {
                            batch.addAll(pendingLogs);
                            List<TimeLog> saved = timeLogRepository.saveAll(pendingLogs);
                            pendingLogs.clear();
                            return saved.size();
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    }, dbExecutor)
                    .orTimeout(DB_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .whenComplete((count, ex) -> {
                        if (ex != null) {
                            log.error("failed to save record(s)", ex);
                            isDbAvailable.set(false);
                            batch.forEach(this::addToPendingQueue);
                        } else {
                            log.info("successfully saved {} record(s)", count);
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
                    }, dbExecutor)
                    .orTimeout(DB_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .whenComplete((dbAvailable, ex) -> {
                        if (ex == null && dbAvailable) {
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
            Future<List<TimeLog>> dbFuture = dbExecutor.submit(() -> timeLogRepository.findAll());
            List<LocalDateTime> result = new ArrayList<>();
            try {
                List<TimeLog> dbLogs = dbFuture.get(DB_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                dbLogs.forEach(log -> result.add(toLocalDateTime(log)));
            } catch (TimeoutException e) {
                log.warn("database query timeout exceeded", e);
                dbFuture.cancel(true);
                throw new DbException("database query timeout exceeded, try later");
            } catch (Exception e) {
                log.error("error getting logs from db: {}", e.getMessage());
                throw new DbException("database query error");
            }
            return result;
        } else {
            throw new DbException("service is unavailable");
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
