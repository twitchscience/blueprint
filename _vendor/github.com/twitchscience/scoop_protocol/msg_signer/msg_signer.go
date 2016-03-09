package msg_signer

import (
	"crypto/hmac"
	"encoding/binary"
	"hash"
	"io"
	"io/ioutil"
	"time"
)

type Signer struct {
	h hash.Hash
}

func NewSigner(h hash.Hash) *Signer {
	return &Signer{
		h: h,
	}
}

func (s *Signer) signature(msg []byte) []byte {
	s.h.Reset()
	s.h.Write(msg)
	return s.h.Sum(nil)
}

func (s *Signer) Sign(msg []byte) []byte {
	b := make([]byte, 8+len(msg))
	binary.PutVarint(b, int64(len(msg)))
	b = append(b[:8], msg...)
	b = append(b, s.signature(b)...)
	return b
}

func (s *Signer) Verify(b []byte) ([]byte, bool) {
	if len(b) < 8 {
		return nil, false
	}

	length, _ := binary.Varint(b[:8])
	if length < 0 {
		return nil, false
	}

	length += 8
	if length > int64(len(b)) {
		return nil, false
	}

	msg := b[8:length]
	signature := b[length:]
	signature2 := s.signature(b[:length])
	return msg, hmac.Equal(signature, signature2)
}

type TimeSigner struct {
	*Signer
}

func NewTimeSigner(h hash.Hash) *TimeSigner {
	return &TimeSigner{
		Signer: NewSigner(h),
	}
}

func (s *TimeSigner) Sign(msg []byte) []byte {
	b := make([]byte, 8+len(msg))
	binary.PutVarint(b, time.Now().Unix())
	b = append(b[:8], msg...)
	return s.Signer.Sign(b)
}

func (s *TimeSigner) Verify(b []byte, dur time.Duration) ([]byte, bool) {
	msg, ok := s.Signer.Verify(b)
	if !ok {
		return nil, ok
	}

	unixTime, _ := binary.Varint(msg[:8])
	if time.Since(time.Unix(unixTime, 0)) > dur {
		return nil, false
	}
	return msg[8:], true
}

func (s *TimeSigner) PagedVerify(r io.Reader, dur time.Duration) ([]byte, bool) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, false
	}
	return s.Verify(b, dur)
}
