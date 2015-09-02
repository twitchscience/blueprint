package auth

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"code.google.com/p/goauth2/oauth"
)

// Manage login via google plus
func (a *GoogleAuth) LoginHandler(w http.ResponseWriter, r *http.Request) {
	user := a.User(r)
	if user != nil {
		// We're logged in already!
		log.Println("Redirecting to main")
		http.Redirect(w, r, r.FormValue("redirect_to"), http.StatusFound)
		return
	}

	session, _ := a.CookieStore.Get(r, "auth-session")
	transport := &oauth.Transport{Config: a.OauthConfig}

	code := r.FormValue("code")
	if code == "" {
		b := make([]byte, 32)
		rand.Read(b)
		state := fmt.Sprintf("%032x", b)
		session.Values["auth-state"] = state
		session.Values["auth-redirect-to"] = r.FormValue("redirect_to")
		session.Save(r, w)
		log.Println("Redirecting to auth code URL")
		http.Redirect(w, r, a.OauthConfig.AuthCodeURL(state), http.StatusFound)
		return
	}

	state := r.FormValue("state")
	expectedStateIface, present := session.Values["auth-state"]
	expectedState, typeOk := expectedStateIface.(string)

	if !present || !typeOk || state != expectedState {
		//Probably a forged request. Drop it on the floor
		http.Error(w, "Invalid request forgery state token", http.StatusInternalServerError)
		return
	}

	_, err := transport.Exchange(code)
	if reportOnError(w, err) {
		return
	}

	response, err := transport.Client().Get(a.ApiUrl)
	if reportOnError(w, err) {
		return
	}

	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if reportOnError(w, err) {
		return
	}

	var gUser googleUser
	err = json.Unmarshal(body, &gUser)
	if reportOnError(w, err) {
		return
	}

	if gUser.Emails == nil {
		// Workaround for the user's first time through the system
		// Google seems to take a few seconds for new privs to propogate through their side (?!?)
		time.Sleep(5 * time.Second)
		http.Redirect(w, r, a.LoginUrl, http.StatusFound)
		return
	}

	session.Values["login-time"] = time.Now().Unix()
	session.Values["login-name"] = gUser.DisplayName
	session.Values["login-email"] = gUser.Emails[0].Value
	session.Save(r, w)
	http.Redirect(w, r, session.Values["auth-redirect-to"].(string), http.StatusFound)
	return
}

type googleEmail struct {
	Value string
	Type  string
}

type googleUser struct {
	DisplayName string
	Emails      []googleEmail
}

func (a *GoogleAuth) LogoutHandler(w http.ResponseWriter, r *http.Request) {
	session, _ := a.CookieStore.Get(r, "auth-session")

	delete(session.Values, "login-time")
	delete(session.Values, "login-name")
	delete(session.Values, "login-email")
	session.Save(r, w)
	http.Redirect(w, r, "/"+r.FormValue("redirect_to"), http.StatusFound)
	return
}

func reportOnError(w http.ResponseWriter, err error) bool {
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return true
	}
	return false
}
