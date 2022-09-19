// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Borrowed from https://github.com/VictoriaMetrics/VictoriaMetrics/tree/v1.81.2/lib/ingestserver/opentsdbhttp
// Changes:
// 2022-09-18
// remove VictoriaMetrics logger library
// remove VictoriaMetrics metrics client library
// return error from MustStart and MustServe

package opentsdbhttp

import (
	"context"
	"go.opentelemetry.io/collector/component"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
)

//var (
//	writeRequests = metrics.NewCounter(`vm_ingestserver_requests_total{type="opentsdbhttp", name="write", net="tcp"}`)
//	writeErrors   = metrics.NewCounter(`vm_ingestserver_request_errors_total{type="opentsdbhttp", name="write", net="tcp"}`)
//)

// Server represents HTTP OpenTSDB server.
type Server struct {
	s  *http.Server
	ln net.Listener
	wg sync.WaitGroup
}

// MustStart starts HTTP OpenTSDB server on the given addr.
//
// MustStop must be called on the returned server when it is no longer needed.
func MustStart(addr string, insertHandler func(r *http.Request) error, host component.Host) (*Server, error) {
	//logger.Infof("starting HTTP OpenTSDB server at %q", addr)
	lnTCP, err := netutil.NewTCPListener("opentsdbhttp", addr, nil)
	if err != nil {
		return nil, err
		//logger.Fatalf("cannot start HTTP OpenTSDB collector at %q: %s", addr, err)
	}
	return MustServe(lnTCP, insertHandler, host), nil
}

// MustServe serves OpenTSDB HTTP put requests from ln.
//
// MustStop must be called on the returned server when it is no longer needed.
func MustServe(ln net.Listener, insertHandler func(r *http.Request) error, host component.Host) *Server {
	h := newRequestHandler(insertHandler)
	hs := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       time.Minute,
		// Do not set ReadTimeout and WriteTimeout here,
		// since these timeouts must be controlled by request handler.
	}
	s := &Server{
		s:  hs,
		ln: ln,
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		err := s.s.Serve(s.ln)
		if err == http.ErrServerClosed {
			return
		}
		if err != nil {
			host.ReportFatalError(err)
			//logger.Fatalf("error serving HTTP OpenTSDB at %q: %s", s.ln.Addr(), err)
		}
	}()
	return s
}

// Wait waits until the server is stopped with MustStop.
func (s *Server) Wait() {
	s.wg.Wait()
}

// MustStop stops HTTP OpenTSDB server.
func (s *Server) MustStop() {
	//logger.Infof("stopping HTTP OpenTSDB server at %q...", s.ln.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.s.Shutdown(ctx); err != nil {
		//logger.Fatalf("cannot close HTTP OpenTSDB server at %q: %s", s.ln.Addr(), err)
	}
	s.wg.Wait()
	//logger.Infof("OpenTSDB HTTP server at %q has been stopped", s.ln.Addr())
}

func newRequestHandler(insertHandler func(r *http.Request) error) http.Handler {
	rh := func(w http.ResponseWriter, r *http.Request) {
		//writeRequests.Inc()
		if err := insertHandler(r); err != nil {
			//writeErrors.Inc()
			httpserver.Errorf(w, r, "%s", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
	return http.HandlerFunc(rh)
}
