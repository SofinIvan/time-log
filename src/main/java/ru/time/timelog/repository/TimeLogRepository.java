package ru.time.timelog.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import ru.time.timelog.model.TimeLog;

@Repository
public interface TimeLogRepository extends JpaRepository<TimeLog, Long> {

    @Query(value = "SELECT 1", nativeQuery = true)
    void checkConnection();
}
