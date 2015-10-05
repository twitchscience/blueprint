/*
	Authorization middleware using github OAuth, with support for using github enterprise.
*/

package auth

import "net/http"

type AuthUser struct {
	Name          string
	IsMemberOfOrg bool
}

type Auth interface {
	AuthorizeOrRedirect(h http.Handler) http.Handler
	AuthorizeOrForbid(h http.Handler) http.Handler
	LoginHandler(w http.ResponseWriter, r *http.Request)
	LogoutHandler(w http.ResponseWriter, r *http.Request)
	AuthCallbackHandler(w http.ResponseWriter, r *http.Request)
	User(r *http.Request) *AuthUser
}
