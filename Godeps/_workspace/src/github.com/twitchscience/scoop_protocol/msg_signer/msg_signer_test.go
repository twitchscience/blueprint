package msg_signer_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"github.com/twitchscience/scoop_protocol/msg_signer"
	"testing"
	"time"
)

func TestSigner(t *testing.T) {
	s := msg_signer.NewSigner(hmac.New(sha256.New, []byte("secret")))
	if _, g := s.Verify(s.Sign([]byte("test1test2test3\n\r"))); !g {
		t.Log("Signer could not self verify\n")
		t.Fail()
	}
	if _, g := s.Verify(s.Sign([]byte("test4test5test6\n\r"))); !g {
		t.Log("Signer could not self verify\n")
		t.Fail()
	}
	x := append([]byte("strin"), s.Sign([]byte("test1test2test3\n\r"))...)
	if _, g := s.Verify(x); g {
		t.Log("Signer falsly verified\n")
		t.Fail()
	}
}

func TestTimeSigner(t *testing.T) {
	s := msg_signer.NewTimeSigner(hmac.New(sha256.New, []byte("secret")))
	if _, g := s.Verify(s.Sign([]byte("test1test2test3\n\r")), time.Second*5); !g {
		t.Log("Signer could not self verify\n")
		t.Fail()
	}
	if _, g := s.Verify(s.Sign([]byte("test4test5test6\n\r")), time.Second*0); g {
		t.Log("Signer failed time duration\n")
		t.Fail()
	}
	x := append([]byte("strin"), s.Sign([]byte("test1test2test3\n\r"))...)
	if _, g := s.Verify(x, time.Second*5); g {
		t.Log("Signer falsly verified\n")
		t.Fail()
	}
}

func TestPagedTimeSigner(t *testing.T) {
	s := msg_signer.NewTimeSigner(hmac.New(sha256.New, []byte("secret")))
	if _, g := s.PagedVerify(bytes.NewReader(s.Sign([]byte("test1test2test3\n\r"))), time.Second*5); !g {
		t.Log("Signer could not self verify\n")
		t.Fail()
	}
	if _, g := s.PagedVerify(bytes.NewReader(s.Sign([]byte("test4test5test6\n\r"))), time.Second*0); g {
		t.Log("Signer failed time duration\n")
		t.Fail()
	}
	x := append([]byte("strin"), s.Sign([]byte("test1test2test3\n\r"))...)
	if _, g := s.PagedVerify(bytes.NewReader(x), time.Second*5); g {
		t.Log("Signer falsly verified\n")
		t.Fail()
	}
}
