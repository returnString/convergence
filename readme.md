# Convergence
![Tests](https://github.com/reservoirdb/convergence/workflows/Test/badge.svg) ![Crates.io](https://img.shields.io/crates/v/convergence)

A set of tools for writing servers that speak PostgreSQL's wire protocol.

🚧 This project is _extremely_ WIP at this stage.

## Crates
`convergence` contains the core traits, protocol handling and connection state machine for emulating a Postgres server.

`convergence-arrow` enables translation of [Apache Arrow](https://arrow.apache.org) dataframes into Postgres result sets, allowing you to access your Arrow-powered data services via standard Postgres drivers. It also provides a reusable `Engine` implementation using [DataFusion](https://github.com/apache/arrow/tree/master/rust/datafusion) for execution.

`convergence-dynamodb` adds support for querying AWS DynamoDB tables via SQL, pushing queries down to the table where possible and using DataFusion to handle additional post-processing of data (e.g. running `group by` on a set of DynamoDB items).
