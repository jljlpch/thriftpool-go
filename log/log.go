package log

import (
	"bytes"
	"strconv"
	"time"

	"github.com/golang/glog"
	"golang.org/x/net/context"
)

// TraceIDKey pass though context
const TraceIDKey = "_TraceId_"

// NewContext transform a normal context to log context
func NewContext() context.Context {
	key := strconv.FormatInt(time.Now().UnixNano(), 10)
	return context.WithValue(context.Background(), TraceIDKey, key)
}

// FromContext transform a normal context to log context
func FromContext(ctx context.Context) context.Context {
	key := strconv.FormatInt(time.Now().UnixNano(), 10)
	return context.WithValue(ctx, TraceIDKey, key)
}

func Info(ctx context.Context, args ...interface{}) {
	if ctx == nil || !hasTraceKey(ctx) {
		glog.Info(args)
		return
	}
	glog.Info(prependParam(args, ctx)...)
}

func Infof(ctx context.Context, format string, args ...interface{}) {
	if ctx == nil || !hasTraceKey(ctx) {
		glog.Infof(format, args)
		return
	}
	glog.Infof(prependFormat(format), prependParam(args, ctx)...)
}

func Warning(ctx context.Context, args ...interface{}) {
	if ctx == nil || !hasTraceKey(ctx) {
		glog.Warning(args)
		return
	}
	glog.Warning(prependParam(args, ctx)...)
}

func Warningf(ctx context.Context, format string, args ...interface{}) {
	if ctx == nil || !hasTraceKey(ctx) {
		glog.Warningf(format, args)
		return
	}
	glog.Warningf(prependFormat(format), prependParam(args, ctx)...)
}

func Error(ctx context.Context, args ...interface{}) {
	if ctx == nil || !hasTraceKey(ctx) {
		glog.Error(args)
		return
	}
	glog.Error(prependParam(args, ctx)...)
}

func Errorf(ctx context.Context, format string, args ...interface{}) {
	if ctx == nil || !hasTraceKey(ctx) {
		glog.Errorf(format, args)
		return
	}
	glog.Errorf(prependFormat(format), prependParam(args, ctx)...)
}

func Fatal(ctx context.Context, args ...interface{}) {
	if ctx == nil || !hasTraceKey(ctx) {
		glog.Fatal(args)
		return
	}
	glog.Fatal(prependParam(args, ctx)...)
}

func Fatalf(ctx context.Context, format string, args ...interface{}) {
	if ctx == nil || !hasTraceKey(ctx) {
		glog.Fatalf(format, args)
		return
	}
	glog.Fatalf(prependFormat(format), prependParam(args, ctx)...)
}

func prependParam(s []interface{}, ctx context.Context) []interface{} {
	return append([]interface{}{ctx.Value(TraceIDKey)}, s...)
}

func prependFormat(f string) string {
	var buffer bytes.Buffer
	buffer.WriteString("[%s] ")
	buffer.WriteString(f)
	return buffer.String()
}

func hasTraceKey(ctx context.Context) bool {
	return ctx.Value(TraceIDKey) != nil
}
