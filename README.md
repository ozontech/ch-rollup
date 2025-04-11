# ch-rollup [![Go Reference](https://pkg.go.dev/badge/github.com/ozontech/ch-rollup.svg)](https://pkg.go.dev/github.com/ozontech/ch-rollup) [![Test](https://github.com/ozontech/ch-rollup/actions/workflows/test.yml/badge.svg)](https://github.com/ozontech/ch-rollup/actions/workflows/test.yml) [![Lint](https://github.com/ozontech/ch-rollup/actions/workflows/lint.yml/badge.svg)](https://github.com/ozontech/ch-rollup/actions/workflows/lint.yml)

What is **ch-rollup**? It is a library that roll up (continuously aggregates) data in ClickHouse. The essence is that when storing time series data with a one-minute interval, we can analyze these data well, but after some time, we no longer need such high precision and would like to reduce this precision to store less data. For example, after storing data for a month, we might want to aggregate it to an hourly level. This means taking sixty points per hour, finding the average RPS/RT/etc, and recording it.

## Documentation

Learn how to use **ch-rollup**.

- [Installation](docs/installation.md)
- [Example](example)
- [Design](docs/design.md)
- [Motivation](docs/motivation.md)

## Known limitations
- [Doesn't support replicas.](https://github.com/ozontech/ch-rollup/issues/7)

## Roadmap

The features we want to do are described in the [issues](https://github.com/ozontech/ch-rollup/issues).

## License

[Apache License Version 2.0](LICENSE)

## Copyright

Copyright 2025 LLC "Ozon Technologies"
