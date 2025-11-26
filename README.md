# solana-shred-perf

A benchmark to compare performance between two shred sources. It shows the win rate and, most importantly, the median latencies for each source when they lose. This gives a clear understanding of whether a particular shred source can provide an advantage.

# How to use?

## Build
```bash
cargo build
```

## Run
```bash
export RUST_LOG=info && cargo run -- --name-0 <Shred1's name> --port-0 <Port to receive Shred1> --name-1 <Shred2's name> --port-1 <Port to receive Shred2>
```

Where:
- `<Shred1's name>` is an arbitrary name for Shred1, used to distinguish it in printed output.
- `<Port to receive Shred1>` is the port to receive Shred1.
- `<Shred2's name>` is an arbitrary name for Shred2, used to distinguish it in printed output.
- `<Port to receive Shred2>` is the port to receive Shred2.

For example:
```bash
export RUST_LOG=info && cargo run -- --name-0 Shreder --port-0 20001 --name-1 Source2 --port-1 20002
```

This compares a shred named `Shreder` with a data receiving port of `20001` to a shred named `Source2` with a data receiving port of `20002`. 

```bash
Port Shreder: 141997 | Port Source2: 141995 | Matched: 141991 | Shreder loses in 18.03466416885578% with median delay : 96.732µs AND Source2 loses in 81.96533583114423% with median delay: 1.650741ms
```