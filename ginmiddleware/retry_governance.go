package ginmiddleware

import (
	"github.com/gin-gonic/gin"

	governance "github.com/Duanyubin/http-governance-client"
)

// GovernanceMiddleware imports chain-wide retry constraints into Request.Context.
func GovernanceMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := governance.ContextWithRetryConstraints(c.Request.Context(), c.Request.Header)
		defer cancel()

		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}
