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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"io"
	"net/http"
	"time"

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

func HttpPutHandler(ctx context.Context, consumer consumer.Metrics) func(req *http.Request) error {
	rowProcessorFunc := processHttpRows(ctx, consumer)
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

func TelnetPutHandler(ctx context.Context, consumer consumer.Metrics) func(r io.Reader) error {
	return func(r io.Reader) error {
		return telnetParser.ParseStream(r, processTelnetRows(ctx, consumer))
	}
}

func processHttpRows(ctx context.Context, consumer consumer.Metrics) func(rows []httpParser.Row) error {
	return func(rows []httpParser.Row) error {
		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		for _, r := range rows {
			m, err := buildHttpMetric(r)
			if err != nil {
				return err
			}
			m.CopyTo(rm.ScopeMetrics().AppendEmpty())
		}
		err := consumer.ConsumeMetrics(ctx, metrics)
		if err != nil {
			return err
		}
		return nil
	}
}

func processTelnetRows(ctx context.Context, consumer consumer.Metrics) func(rows []telnetParser.Row) error {
	return func(rows []telnetParser.Row) error {
		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		for _, r := range rows {
			m, err := buildTelnetMetric(r)
			if err != nil {
				return err
			}
			m.CopyTo(rm.ScopeMetrics().AppendEmpty())
		}
		err := consumer.ConsumeMetrics(ctx, metrics)
		if err != nil {
			return err
		}
		return nil
	}
}

func buildHttpMetric(row httpParser.Row) (pmetric.ScopeMetrics, error) {
	ilm := pmetric.NewScopeMetrics()
	nm := ilm.Metrics().AppendEmpty()
	nm.SetName(row.Metric)
	nm.SetEmptyGauge()
	//dp := nm.Sum().DataPoints().AppendEmpty()
	g := nm.Gauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, row.Timestamp*1000000)))
	dp.SetDoubleVal(row.Value)
	for _, v := range row.Tags {
		dp.Attributes().PutString(v.Key, v.Value)
	}
	return ilm, nil
}

func buildTelnetMetric(row telnetParser.Row) (pmetric.ScopeMetrics, error) {
	ilm := pmetric.NewScopeMetrics()
	nm := ilm.Metrics().AppendEmpty()
	nm.SetName(row.Metric)
	//dp := nm.Sum().DataPoints().AppendEmpty()
	g := nm.Gauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(row.Timestamp, 0)))
	dp.SetDoubleVal(row.Value)
	for _, v := range row.Tags {
		dp.Attributes().PutString(v.Key, v.Value)
	}
	return ilm, nil
}

func (r *opentsdbReceiver) Start(ctx context.Context, host component.Host) error {
	r.logger.Info("starting OpenTSDB receiver")

	var err error
	if len(r.config.ListenerAddr) > 0 {
		r.telnetServer, err = opentsdb.MustStart(r.config.ListenerAddr,
			TelnetPutHandler(ctx, r.nextConsumer),
			HttpPutHandler(ctx, r.nextConsumer),
			host)
		if err != nil {
			return err
		}
	} else if len(r.config.HttpListenerAddr) > 0 {
		//telnet starts http handler on same port, so this is only used if telnet is disabled
		r.httpServer, err = opentsdbhttp.MustStart(r.config.HttpListenerAddr,
			HttpPutHandler(ctx, r.nextConsumer),
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
