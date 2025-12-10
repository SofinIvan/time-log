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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
@RequiredArgsConstructor
public class TimeLogService {

    private static final ZoneId ZONE_ID = ZoneId.systemDefault();
    private static final int MAX_QUEUE_SIZE = 10000;
    private static final int BATCH_SIZE = 100;

    private final AtomicBoolean isDbAvailable = new AtomicBoolean(true);
    private final AtomicInteger failedAttempts = new AtomicInteger(0);
    private final BlockingQueue<TimeLog> pendingLogs = new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);

    private final TimeLogRepository timeLogRepository;

    @Scheduled(cron = "${schedule.db.record.cron}")
    public void record() {
        log.debug("Recording time log to db");
        TimeLog timeLog = createTimeLog();
        try {
            if (isDbAvailable.get()) {
                timeLogRepository.save(timeLog);
            } else {
                log.warn("db is unavailable, time logs added to queue");
                addToPendingQueue(timeLog);
            }
        } catch (Exception e) {
            log.error("error in record method when writing to db", e);
            isDbAvailable.set(false);
            addToPendingQueue(timeLog);
        }
    }

    @Scheduled(cron = "${schedule.db.pending.cron}")
    public void recordFailedFromPending() {
        if (isDbAvailable.get() && !pendingLogs.isEmpty()) {
            List<TimeLog> batch = new ArrayList<>(BATCH_SIZE);
            pendingLogs.drainTo(batch, BATCH_SIZE);
            try {
                timeLogRepository.saveAll(batch);
                log.info("successfully saved {} pending records", batch.size());
            } catch (Exception e) {
                log.error("failed to save pending records batch", e);
                isDbAvailable.set(false);
                batch.forEach(this::addToPendingQueue);
            }
        }
    }

    @Scheduled(cron = "${schedule.db.check.cron}")
    public void restoreConnection() {
        if (!isDbAvailable.get()) {
            try {
                timeLogRepository.checkConnection();
                isDbAvailable.set(true);
                failedAttempts.set(0);
                log.info("db is available, connection restored");
            } catch (Exception e) {
                int attempt = failedAttempts.incrementAndGet();
                log.warn("db still unavailable, attempt: {}", attempt);
            }
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
        timeLog.setCreated(LocalDateTime.now());
        return timeLog;
    }

}
