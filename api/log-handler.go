package api

import (
	"net/http"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta"
)

type logHandler struct {
	handler http.Handler
}

func (l logHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Serves the request.
	helper.Logger.Info(r.Context(), "STARTING", r.Method, r.Host, r.URL)
	l.handler.ServeHTTP(w, r)
	helper.Logger.Info(r.Context(), "COMPLETED", r.Method, r.Host, r.URL)
}

func SetLogHandler(h http.Handler, _ *meta.Meta) http.Handler {
	return logHandler{handler: h}
}
