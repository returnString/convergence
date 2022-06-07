# Convergence
![Tests](https://github.com/returnString/convergence/workflows/Test/badge.svg) ![Crates.io](https://img.shields.io/crates/v/convergence)

A set of tools for writing servers that speak PostgreSQL's wire protocol.

🚧 This project is _extremely_ WIP at this stage.

## Crates
`convergence` contains the core traits, protocol handling and connection state machine for emulating a Postgres server.

`convergence-arrow` enables translation of [Apache Arrow](https://arrow.apache.org) dataframes into Postgres result sets, allowing you to access your Arrow-powered data services via standard Postgres drivers. It also provides a reusable `Engine` implementation using [DataFusion](https://github.com/apache/arrow-datafusion) for execution.
