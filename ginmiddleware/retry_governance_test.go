package ginmiddleware

import (
	stdhttp "net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	governance "github.com/Duanyubin/http-governance-client"
)

func TestGovernanceMiddlewareImportsRetryConstraints(t *testing.T) {
	gin.SetMode(gin.TestMode)
	deadline := time.Now().Add(time.Minute).Truncate(time.Millisecond)

	var gotNoMoreRetry bool
	var gotDeadline time.Time
	router := gin.New()
	router.Use(GovernanceMiddleware())
	router.GET("/", func(c *gin.Context) {
		gotNoMoreRetry = governance.NoMoreRetryFromContext(c.Request.Context())
		gotDeadline, _ = c.Request.Context().Deadline()
		c.Status(stdhttp.StatusNoContent)
	})

	request := httptest.NewRequest(stdhttp.MethodGet, "/", nil)
	request.Header.Set(governance.HeaderNoMoreRetry, "true")
	request.Header.Set(governance.HeaderRequestDeadline, strconv.FormatInt(deadline.UnixMilli(), 10))
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != stdhttp.StatusNoContent {
		t.Fatalf("response status = %d, want %d", response.Code, stdhttp.StatusNoContent)
	}
	if !gotNoMoreRetry {
		t.Fatal("NoMoreRetryFromContext() = false, want true")
	}
	if !gotDeadline.Equal(deadline) {
		t.Fatalf("Context deadline = %v, want %v", gotDeadline, deadline)
	}
}
