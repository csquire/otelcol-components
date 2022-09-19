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

package opentsdb

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
	"io"
	"net/http"

	telnetParser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/opentsdb"
	httpParser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/opentsdbhttp"
	"github.com/csquire/otelcol-components/receiver/opentsdb/lib/ingestserver/opentsdb"
	"github.com/csquire/otelcol-components/receiver/opentsdb/lib/ingestserver/opentsdbhttp"
)

type opentsdbReceiver struct {
	telnetServer *opentsdb.Server
	httpServer   *opentsdbhttp.Server

	logger       *zap.Logger
	nextConsumer consumer.Metrics
	config       *Config

	settings component.ReceiverCreateSettings
}

func HttpPutHandler(consumer consumer.Metrics) func(req *http.Request) error {
	rowProcessorFunc := processHttpRows(consumer)
	return func(req *http.Request) error {
		path := req.URL.Path
		switch path {
		case "/api/put":
			return httpParser.ParseStream(req, func(rows []httpParser.Row) error {
				return rowProcessorFunc(rows)
			})
		default:
			return fmt.Errorf("unexpected path: %q", path)
		}
	}
}

func TelnetPutHandler(consumer consumer.Metrics) func(r io.Reader) error {
	return func(r io.Reader) error {
		return telnetParser.ParseStream(r, processTelnetRows(consumer))
	}
}

func mapTags(tags []httpParser.Tag) []telnetParser.Tag {
	var t []telnetParser.Tag
	for _, v := range tags {
		t = append(t, telnetParser.Tag{Key: v.Key, Value: v.Value})
	}
	return t
}

func processHttpRows(consumer consumer.Metrics) func(rows []httpParser.Row) error {
	return func(rows []httpParser.Row) error {
		for _, r := range rows {
			if err := processRow(telnetParser.Row{
				Metric:    r.Metric,
				Tags:      mapTags(r.Tags),
				Timestamp: r.Timestamp,
				Value:     r.Value,
			}); err != nil {
				return err
			}

		}
		return nil
	}
}

func processTelnetRows(consumer consumer.Metrics) func(rows []telnetParser.Row) error {
	return func(rows []telnetParser.Row) error {
		for _, r := range rows {
			if err := processRow(r); err != nil {
				return err
			}
		}
		return nil
	}
}

func processRow(row telnetParser.Row) error {
	fmt.Printf("%v\n", row)
	return nil
}

func (r *opentsdbReceiver) Start(_ context.Context, host component.Host) error {
	r.logger.Info("starting OpenTSDB receiver")

	var err error
	if len(r.config.ListenerAddr) > 0 {
		r.telnetServer, err = opentsdb.MustStart(r.config.ListenerAddr,
			TelnetPutHandler(r.nextConsumer),
			HttpPutHandler(r.nextConsumer),
			host)
		if err != nil {
			return err
		}
	}
	if len(r.config.HttpListenerAddr) > 0 {
		r.httpServer, err = opentsdbhttp.MustStart(r.config.HttpListenerAddr,
			HttpPutHandler(r.nextConsumer),
			host)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *opentsdbReceiver) Shutdown(_ context.Context) error {
	if r.telnetServer != nil {
		r.telnetServer.MustStop()
	}
	if r.httpServer != nil {
		r.httpServer.MustStop()
	}
	return nil
}
