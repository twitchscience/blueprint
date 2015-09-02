package auth

import "net/http"

type AuthUser struct {
	Email string
	Name  string
}

type Auth interface {
	UserMiddleware(h http.Handler) http.Handler
	AdminMiddleware(h http.Handler) http.Handler
	User(r *http.Request) *AuthUser
	IsAdmin(r *http.Request) bool
	RequireUser(w http.ResponseWriter, r *http.Request) *AuthUser
	RequireAdmin(w http.ResponseWriter, r *http.Request) bool
	LoginHandler(w http.ResponseWriter, r *http.Request)
	LogoutHandler(w http.ResponseWriter, r *http.Request)
}
