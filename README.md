# IAVL (Immutable AVL Tree)

[![Work in Progress](https://img.shields.io/badge/Status-Under_Development-orange.svg)]()
[![Alpha Release](https://img.shields.io/badge/Release-Alpha-red.svg)]()

> **WARNING: UNDER DEVELOPMENT**
>
> This crate is currently in **alpha** and under active development. Expect API changes, instabilities, and potential bugs. It is not yet ready for production use.

A Rust implementation of a versioned, immutable AVL Tree, heavily inspired by [Cosmos's IAVL crate](https://github.com/cosmos/iavl). 

## Features

- **Immutable & Mutable Interfaces**: Exposes `ImmutableTree` for read-only versioned querying and `MutableTree` for tree modifications and state progression.
- **Versioned Key-Value Storage**: Maintain historical states of the tree efficiently, enabling queries across different state versions.
- **Generic Database Backend**: Built around flexible `KVStore`, `MutKVStore`, and `KVIterator` traits, making it easily adaptable to custom storage engines.
- **Drop-in `redb` Support**: Provides an optional backend implementation for [`redb`](https://github.com/cberner/redb), a pure-Rust embedded key-value store (enable via the `redb` feature flag).
- **Cryptographic Hashing**: Provides built-in SHA-256 node hashing to compute state roots and ensure tree integrity.
- **Memory Efficient**: Utilizes `bytes` and `nebz` (`NonEmptyBz`) crates for optimized, zero-copy-friendly memory allocation and byte slicing.
- **Modern Rust**: Written targeting the Rust 2024 edition.

## Usage

This project is currently pre-release. To test it out, add the following to your `Cargo.toml`:

```toml
[dependencies]
iavl = "0.1.0-alpha.2"
```

To enable the `redb` backend feature:
```toml
[dependencies]
iavl = { version = "0.1.0-alpha.2", features = ["redb"] }
```

## Architecture Overview

The core operations pivot around two primary tree models:

1. **`MutableTree<DB>`**: Used for applying updates. Exposes methods like `insert()`, `remove()`, and `save()`. When `save()` is called, changes are persisted to the underlying database and a new version identifier is established.
2. **`ImmutableTree<DB>`**: Used for querying a specific, historical state of the tree. Operations on an `ImmutableTree` do not modify the tree, making it ideal for concurrent read-only operations and generating state proofs.

## Inspiration

This implementation draws its concepts and fundamental mechanics from [Cosmos's IAVL](https://github.com/cosmos/iavl) in Go. It marries a balanced AVL tree with cryptographic Merkle tree properties to support deterministic state hashing and historical version queries commonly required by blockchain state layers.

## License

This project is dual-licensed under [Apache 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT).
