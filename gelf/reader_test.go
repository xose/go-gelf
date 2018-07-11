package gelf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	kSourceCount  = 5
	kMsgPerSource = 1000
	kMaxMsgLength = 3000
	kChunkSplit   = 1024

	kSleep = 1
)

func TestStressReader(t *testing.T) {
	reader, err := NewReader(":0")
	if err != nil {
		t.Fatal(err)
	}

	addr, err := net.ResolveUDPAddr("udp", reader.Addr())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Listening on %s", addr)

	var sentplain, sentchunk, refused, received, recok, recerr, recpanic int32

	g, _ := errgroup.WithContext(context.Background())
	for i := 0; i < kSourceCount; i++ {
		i := i

		t.Logf("Starting source %d", i)
		conn, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			t.Fatal(err)
		}

		g.Go(func() error {
			defer t.Logf("End source %d", i)
			defer conn.Close()

			c := string('a' + i)
			for i := 0; i < kMsgPerSource; i++ {
				clen := rand.Intn(kMaxMsgLength)
				str := strings.Repeat(c, clen)

				msg := map[string]interface{}{
					"version":       "1.1",
					"timestamp":     0.0,
					"host":          "test",
					"short_message": str,

					"_char": c,
					"_len":  clen,
				}

				enc, err := json.Marshal(msg)
				if err != nil {
					return err
				}

				if len(enc) > kChunkSplit {
					msgId := make([]byte, 8)
					if n, err := rand.Read(msgId); err != nil || len(msgId) != n {
						return err
					}

					for chunk := 0; chunk*kChunkSplit < len(enc); chunk++ {
						start := chunk * kChunkSplit
						end := (chunk + 1) * kChunkSplit
						if end > len(enc) {
							end = len(enc)
						}

						payload := bytes.NewBuffer([]byte{0x1e, 0x0f})
						payload.Write(msgId)
						payload.Write([]byte{uint8(chunk)})
						payload.Write([]byte{uint8(len(enc)/kChunkSplit) + 1})
						payload.Write(enc[start:end])

						if _, err := conn.Write(payload.Bytes()); err != nil {
							atomic.AddInt32(&refused, 1)
						}
					}

					time.Sleep(kSleep * time.Millisecond)
					atomic.AddInt32(&sentchunk, 1)
					continue
				}

				if _, err := conn.Write(enc); err != nil {
					atomic.AddInt32(&refused, 1)
				}

				time.Sleep(kSleep * time.Millisecond)
				atomic.AddInt32(&sentplain, 1)
			}
			return nil
		})
	}

	g.Go(func() error {
		cmsg := make(chan *Message, 1)
		cerr := make(chan error, 1)

		timer := time.NewTimer(0)

		for {
			go func() {
				defer func() {
					if r := recover(); r != nil {
						atomic.AddInt32(&received, 1)
						atomic.AddInt32(&recpanic, 1)
						cerr <- fmt.Errorf("panic: %s", r)
					}
				}()

				msg, err := reader.ReadMessage()
				atomic.AddInt32(&received, 1)
				if err != nil {
					cerr <- err
				} else {
					cmsg <- msg
				}
			}()

			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(1 * time.Second)
			select {
			case msg := <-cmsg:
				c, ok := msg.Extra["_char"].(string)
				if !ok || len(c) != 1 {
					atomic.AddInt32(&recerr, 1)
					t.Errorf("invalid char")
					continue
				}

				clen, ok := msg.Extra["_len"].(float64)
				if !ok || clen < 0 {
					atomic.AddInt32(&recerr, 1)
					t.Errorf("invalid len")
					continue
				}

				if msg.Short != strings.Repeat(c, int(clen)) {
					atomic.AddInt32(&recerr, 1)
					t.Error("msg does not match")
					continue
				}

				atomic.AddInt32(&recok, 1)
			case err := <-cerr:
				atomic.AddInt32(&recerr, 1)
				t.Error(err)
			case <-timer.C:
				return nil
			}

		}
	})

	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d sent (%d plain, %d chunked), %d refused, %d received, (%d OK, %d errors, %d panics)",
		sentplain+sentchunk, sentplain, sentchunk, refused, received, recok, recerr, recpanic)
}
