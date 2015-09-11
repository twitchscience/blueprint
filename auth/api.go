package auth

import "net/http"

type AuthUser struct {
	Name          string
	IsMemberOfOrg bool
}

type Auth interface {
	UserMiddleware(h http.Handler) http.Handler
	User(r *http.Request) *AuthUser
	RequireUser(w http.ResponseWriter, r *http.Request) *AuthUser
	LoginHandler(w http.ResponseWriter, r *http.Request)
	LogoutHandler(w http.ResponseWriter, r *http.Request)
	AuthCallbackHandler(w http.ResponseWriter, r *http.Request)
}
