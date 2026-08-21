package logging

import (
	"context"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"time"

	charmlog "github.com/charmbracelet/log"
)

const (
	ComponentApp                = "app"
	ComponentUpstreamEnrichment = "upstream.enrichment"
	ComponentUpstreamAnalytics  = "upstream.analytics"
)

const serviceName = "telemetryprocessor"

type Format string

const (
	FormatJSON   Format = "json"   // deployed: AWS-style structured JSON
	FormatPretty Format = "pretty" // development: colorized, tag-prefixed console
)

func New(w io.Writer, level slog.Level, format Format, environment string) *slog.Logger {
	var handler slog.Handler
	switch format {
	case FormatPretty:
		// [env][service][package][method] prefix; charm renders the rest.
		handler = &tagHandler{inner: charmlog.NewWithOptions(w, charmlog.Options{
			Level:           charmlog.Level(level),
			ReportTimestamp: true,
			TimeFormat:      "15:04:05.000",
		})}
	default:
		handler = slog.NewJSONHandler(w, &slog.HandlerOptions{
			Level:       level,
			ReplaceAttr: replaceAttr,
		})
	}
	return slog.New(handler).With(
		slog.String("service", serviceName),
		slog.String("environment", environment),
	)
}

func Component(root *slog.Logger, component string) *slog.Logger {
	return root.With(slog.String("component", component))
}

func ResolveFormat(logFormat string, local bool) Format {
	switch strings.ToLower(strings.TrimSpace(logFormat)) {
	case "pretty", "console", "text":
		return FormatPretty
	case "json":
		return FormatJSON
	}
	if local {
		return FormatPretty
	}
	return FormatJSON
}

type tagHandler struct {
	inner             slog.Handler
	env, service, pkg string
}

func (h *tagHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *tagHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	nh := *h
	passthrough := attrs[:0:0]
	for _, a := range attrs {
		switch a.Key {
		case "environment":
			nh.env = a.Value.String()
		case "service":
			nh.service = a.Value.String()
		case "component":
			nh.pkg = a.Value.String()
		default:
			passthrough = append(passthrough, a)
		}
	}
	nh.inner = h.inner.WithAttrs(passthrough)
	return &nh
}

func (h *tagHandler) WithGroup(name string) slog.Handler {
	nh := *h
	nh.inner = h.inner.WithGroup(name)
	return &nh
}

func (h *tagHandler) Handle(ctx context.Context, r slog.Record) error {
	pcPkg, method := callSite(r.PC)
	pkg := h.pkg
	if pkg == "" {
		pkg = pcPkg
	}

	prefix := tag(h.env) + tag(h.service) + tag(pkg) + tag(method)
	msg := r.Message
	if prefix != "" {
		msg = prefix + " " + msg
	}

	nr := slog.NewRecord(r.Time, r.Level, msg, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		nr.AddAttrs(a)
		return true
	})
	return h.inner.Handle(ctx, nr)
}

func tag(s string) string {
	if s == "" {
		return ""
	}
	return "[" + s + "]"
}

func callSite(pc uintptr) (pkg, method string) {
	if pc == 0 {
		return "", ""
	}
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return "", ""
	}
	name := fn.Name()
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:] // enrichment.(*Client).Enrich
	}
	segments := strings.Split(name, ".")
	if len(segments) < 2 {
		return name, ""
	}
	pkg = segments[0]
	for i := len(segments) - 1; i >= 1; i-- {
		if isClosureMarker(segments[i]) || strings.HasPrefix(segments[i], "(") {
			continue
		}
		return pkg, segments[i]
	}
	return pkg, ""
}

func isClosureMarker(s string) bool {
	if !strings.HasPrefix(s, "func") {
		return false
	}
	digits := s[len("func"):]
	if digits == "" {
		return false
	}
	for _, r := range digits {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func replaceAttr(_ []string, a slog.Attr) slog.Attr {
	switch a.Key {
	case slog.TimeKey:
		a.Key = "timestamp"
		if t, ok := a.Value.Any().(time.Time); ok {
			a.Value = slog.StringValue(t.UTC().Format("2006-01-02T15:04:05.000Z07:00"))
		}
	case slog.MessageKey:
		a.Key = "message"
	}
	return a
}
