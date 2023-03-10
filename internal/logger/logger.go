package logger

import (
	"simple-replicator/internal/config"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var zapper *zap.Logger

func init() {
	logCfg := zap.NewProductionConfig()
	if strings.ToLower(config.GetLevel()) == "debug" {
		logCfg.Level.SetLevel(zap.DebugLevel)
	}
	logCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zapper = zap.Must(logCfg.Build())
}

func Debug(msg string, keysAndValues ...interface{}) {
	zapper.Sugar().Debugw(msg, keysAndValues...)
}

func Info(msg string, keysAndValues ...interface{}) {
	zapper.Sugar().Infow(msg, keysAndValues...)
}

func Warn(msg string, keysAndValues ...interface{}) {
	zapper.Sugar().Warnw(msg, keysAndValues...)
}

func Error(msg string, keysAndValues ...interface{}) {
	zapper.Sugar().Errorw(msg, keysAndValues...)
}

func Fatal(msg string, keysAndValues ...interface{}) {
	zapper.Sugar().Fatalw(msg, keysAndValues...)
}
