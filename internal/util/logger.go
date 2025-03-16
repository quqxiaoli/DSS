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

// NewLogger 函数用于创建一个新的日志记录器实例。
// 参数 path 是日志文件的路径，函数将根据该路径打开或创建日志文件。
// 返回一个指向 Logger 结构体的指针和一个错误对象。如果打开文件成功，返回日志记录器实例和 nil 错误；如果出现错误，返回 nil 和相应的错误信息。
func NewLogger(path string) (*Logger, error) {
	// 尝试打开指定路径的日志文件，使用 os.O_APPEND 以追加模式打开，os.O_CREATE 若文件不存在则创建，os.O_WRONLY 以只写模式打开，0644 是文件权限设置。
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// 检查文件打开是否失败，如果失败则返回错误信息。
	if err != nil {
		return nil, fmt.Errorf("failed to open log file %s: %v", path, err)
	}

	// 从日志文件路径中提取端口号信息，假设路径格式为包含 "_" 和 ".log" 的字符串，通过分割和去除后缀获取端口号。
	port := strings.Split(path, "_")[1]
	port = strings.TrimSuffix(port, ".log")
	// 创建一个新的 Logger 结构体实例，初始化 infoLogger 和 errorLogger，并将打开的文件赋值给 file 字段。
	l := &Logger{
		// 创建一个信息日志记录器，设置日志前缀为 [INFO localhost:端口号]，并包含标准的日期和时间标志。
		infoLogger: log.New(file, fmt.Sprintf("[INFO localhost:%s] ", port), log.LstdFlags),
		// 创建一个错误日志记录器，设置日志前缀为 [ERROR localhost:端口号]，并包含标准的日期和时间标志。
		errorLogger: log.New(file, fmt.Sprintf("[ERROR localhost:%s] ", port), log.LstdFlags),
		// 将打开的日志文件赋值给 Logger 结构体的 file 字段。
		file: file,
	}
	// 返回创建好的日志记录器实例和 nil 错误，表示创建成功。
	return l, nil
}

func (l *Logger) Info(format string, v ...interface{}) {
	// 调用 logWithRateLimit 方法，传入信息日志记录器、日志格式和参数
	l.logWithRateLimit(l.infoLogger, format, v...)
}

func (l *Logger) Error(format string, v ...interface{}) {
	// 加锁，保证并发安全，避免多个 goroutine 同时操作 errorLogger 导致数据混乱
	l.mu.Lock()
	// 使用 defer 确保在函数结束时解锁，防止死锁
	defer l.mu.Unlock()
	// 使用 errorLogger 按照指定的格式和参数记录错误日志
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

// logWithRateLimit 方法用于对日志记录进行速率限制，避免短时间内频繁记录日志。
// 参数 logger 是要使用的日志记录器，format 是日志的格式字符串，v 是可变参数，用于填充格式字符串。
func (l *Logger) logWithRateLimit(logger *log.Logger, format string, v ...interface{}) {
	// 加锁，确保并发安全，避免多个 goroutine 同时修改 lastLogTime 字段
	l.mu.Lock()
	// 函数结束时解锁
	defer l.mu.Unlock()
	// 获取当前时间
	now := time.Now()
	// 检查当前时间与上一次记录日志的时间间隔是否小于 2 秒
	if now.Sub(l.lastLogTime) < 100*time.Millisecond {
		// 如果时间间隔小于 100ms，则不记录日志，直接返回
		return
	}
	// 更新上一次记录日志的时间为当前时间
	l.lastLogTime = now
	// 使用传入的日志记录器按照指定格式记录日志
	logger.Printf(format, v...)
}

// 新增：无限制日志方法，用于关键操作
func (l *Logger) InfoNoLimit(format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infoLogger.Printf(format, v...)
}
