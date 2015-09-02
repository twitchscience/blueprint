/*
Mock version of the auth system for testing

See New() for the real one
*/

package auth

import "net/http"

type MockAuth struct {
	IsMockUserAdmin bool
	MockUser        *AuthUser
}

func NewMock(isUserAdmin bool, user *AuthUser) Auth {
	return &MockAuth{
		IsMockUserAdmin: isUserAdmin,
		MockUser:        user,
	}
}

func (m *MockAuth) UserMiddleware(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if m.MockUser == nil {
			http.Error(w, "Nope", http.StatusUnauthorized)
			return
		}

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func (m *MockAuth) AdminMiddleware(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if !m.IsAdmin(r) {
			http.Error(w, "Nope", http.StatusUnauthorized)
			return
		}

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func (m *MockAuth) User(r *http.Request) *AuthUser {
	return m.MockUser
}

func (m *MockAuth) IsAdmin(r *http.Request) bool {
	return m.MockUser != nil && m.IsMockUserAdmin
}

func (m *MockAuth) RequireUser(w http.ResponseWriter, r *http.Request) *AuthUser {
	return m.MockUser
}

func (m *MockAuth) RequireAdmin(w http.ResponseWriter, r *http.Request) bool {
	return m.IsMockUserAdmin
}

func (m *MockAuth) LoginHandler(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, r.FormValue("redirect_to"), http.StatusFound)
	return
}

func (m *MockAuth) LogoutHandler(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, r.FormValue("redirect_to"), http.StatusFound)
	return
}
