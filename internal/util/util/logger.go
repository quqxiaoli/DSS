package util

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type Logger struct {
    infoLogger  *log.Logger
    errorLogger *log.Logger
    file        *os.File
    mu          sync.Mutex
    lastLogTime time.Time
}

func NewLogger(path string) (*Logger, error) {
    file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return nil, fmt.Errorf("failed to open log file %s: %v", path, err)
    }

    port := strings.Split(path, "_")[1]
    port = strings.TrimSuffix(port, ".log")
    l := &Logger{
        infoLogger:  log.New(file, fmt.Sprintf("[INFO localhost:%s] ", port), log.LstdFlags),
        errorLogger: log.New(file, fmt.Sprintf("[ERROR localhost:%s] ", port), log.LstdFlags),
        file:        file,
    }
    return l, nil
}

func (l *Logger) Info(format string, v ...interface{}) {
    l.logWithRateLimit(l.infoLogger, format, v...)
}

func (l *Logger) Error(format string, v ...interface{}) {
    l.mu.Lock()
    defer l.mu.Unlock()
    l.errorLogger.Printf(format, v...)
}

func (l *Logger) InfoLogger() *log.Logger {
    return l.infoLogger
}

func (l *Logger) ErrorLogger() *log.Logger {
    return l.errorLogger
}

func (l *Logger) Close() {
    l.file.Close()
}

func (l *Logger) logWithRateLimit(logger *log.Logger, format string, v ...interface{}) {
    l.mu.Lock()
    defer l.mu.Unlock()
    now := time.Now()
    if now.Sub(l.lastLogTime) < 2*time.Second {
        return
    }
    l.lastLogTime = now
    logger.Printf(format, v...)
}