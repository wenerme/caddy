package integration

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddytest"
)

func TestReverseProxyRequestBodyIdleClosesHTTP1Connection(t *testing.T) {
	const (
		bodySize = 1024
		idle     = 1 * time.Second
	)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	tester := caddytest.NewTester(t)
	tester.InitServer(fmt.Sprintf(`{
		"admin": {"listen": "localhost:2999", "config": {"persist": false}},
		"apps": {"http": {"servers": {"srv0": {
			"listen": ["127.0.0.1:9080"],
			"automatic_https": {"disable": true},
			"read_idle_timeout": %d,
			"routes": [{"handle": [{"handler": "reverse_proxy", "upstreams": [{"dial": %q}]}]}]
		}}}}
	}`, idle.Nanoseconds(), upstream.Listener.Addr().String()), "json")
	if t.Failed() {
		t.FailNow()
	}

	for _, tc := range []struct {
		name       string
		finishBody bool
		readFirst  bool
	}{
		{name: "finish body before reading response", finishBody: true},
		{name: "read response before finishing body", finishBody: true, readFirst: true},
		{name: "leave body unfinished", finishBody: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", "127.0.0.1:9080", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			reader := bufio.NewReader(conn)
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}

			_, err = fmt.Fprintf(conn, "POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: %d\r\n\r\n", bodySize)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = conn.Write([]byte{'x'}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(idle + 200*time.Millisecond)

			var first *http.Response
			readResponse := func() error {
				var err error
				first, err = http.ReadResponse(reader, &http.Request{Method: http.MethodPost})
				return err
			}
			if tc.readFirst {
				if err := readResponse(); err != nil {
					t.Fatalf("read timed-out response: %v", err)
				}
			}
			if tc.finishBody {
				if _, err = conn.Write(bytes.Repeat([]byte{'x'}, bodySize-1)); err != nil {
					// A correct fix may close the connection before the remainder is sent.
					if first == nil {
						return
					}
				}
			}
			if first == nil {
				if err := readResponse(); err != nil {
					t.Fatalf("read timed-out response: %v", err)
				}
			}
			if first.StatusCode != http.StatusBadRequest && first.StatusCode != http.StatusGatewayTimeout && first.StatusCode != 499 {
				t.Fatalf("timed-out response status = %d, want 400, 504, or 499", first.StatusCode)
			}
			if err := first.Body.Close(); err != nil {
				t.Fatal(err)
			}

			connectionClosed := first.Close || strings.EqualFold(first.Header.Get("Connection"), "close")
			if !connectionClosed {
				_, err = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
				if err == nil {
					second, readErr := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
					if readErr == nil {
						defer second.Body.Close()
						t.Fatalf("timed-out response kept HTTP/1.1 connection alive; second response status = %d", second.StatusCode)
					}
					if !errors.Is(readErr, io.EOF) && !errors.Is(readErr, net.ErrClosed) {
						t.Fatalf("read after apparently reusable timed-out connection: %v", readErr)
					}
				}
			}
		})
	}

	resp, err := http.Get("http://127.0.0.1:9080/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("new connection GET status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestNetHTTPBodyDeadlineCancelsRequestContext(t *testing.T) {
	contextCanceled := make(chan bool, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := http.NewResponseController(w).SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			t.Errorf("set read deadline: %v", err)
			return
		}
		_, err := io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
			contextCanceled <- true
		default:
			contextCanceled <- false
		}
		if err == nil {
			t.Error("body read unexpectedly succeeded")
		}
		w.WriteHeader(499)
	}))
	defer srv.Close()

	conn, err := net.DialTimeout("tcp", srv.Listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = fmt.Fprintf(conn, "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\nx"); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	_, _ = http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
	select {
	case canceled := <-contextCanceled:
		if !canceled {
			t.Fatal("net/http did not cancel the request context after a body read deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("net/http handler did not finish")
	}
}
