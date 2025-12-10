package ru.time.timelog.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

import java.time.LocalDateTime;

@Entity
@Table(schema = "public", name = "time_log")
@Data
public class TimeLog {
    @Id
    private Long id;

    private LocalDateTime created;
}
