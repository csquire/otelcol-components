# OpenTelemetry Collector Components

## OpenTSDB Receiver
Receiver to accept OpenTSDB metrics by accepting OpentTSDB style telnet and http API requests.

## Building and Testing
In the `otelcol-dev` directory:

build:
```
go build -o otelcol-dev
```

run:
```
./otelcol-dev --config config.yaml
```

## Building Custom Collector
Use the instructtion [here](https://opentelemetry.io/docs/collector/custom-collector/) to download the `ocb` utility. 
With this utility you can regenerate the `otelcol-dev` collector to use for testing the components.

