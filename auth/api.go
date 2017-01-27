/*
	Authorization middleware using github OAuth, with support for using github enterprise.
*/

package auth

import (
	"net/http"

	"github.com/zenazn/goji/web"
)

// User represents a user for authorization purposes
type User struct {
	Name          string
	IsMemberOfOrg bool
	IsAdmin       bool
}

// Auth is the interface managing user auth flow
type Auth interface {
	AuthorizeOrForbid(c *web.C, h http.Handler) http.Handler
	AuthorizeOrForbidAdmin(c *web.C, h http.Handler) http.Handler
	ExpireDisplayName(h http.Handler) http.Handler
	LoginHandler(w http.ResponseWriter, r *http.Request)
	LogoutHandler(w http.ResponseWriter, r *http.Request)
	AuthCallbackHandler(w http.ResponseWriter, r *http.Request)
	User(r *http.Request) *User
}
