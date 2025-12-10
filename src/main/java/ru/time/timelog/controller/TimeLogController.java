package ru.time.timelog.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.time.timelog.service.TimeLogService;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/api/v1/time-log")
@RequiredArgsConstructor
public class TimeLogController {

    private final TimeLogService timeLogService;

    @GetMapping
    public List<LocalDateTime> getTimeLog() {
        return timeLogService.getLogs();
    }
}
