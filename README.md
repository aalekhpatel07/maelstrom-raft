# An implementation of Raft (w/ Maelstrom)

The [Raft Consensus Algorithm](https://raft.github.io/) is one of the simplest algorithms/protocols to form a consensus in a ditributed network of nodes. It provides a way for a majority of the nodes of a cluster to agree upon the values stored in a linear log (i.e. list of messages), and replicating the log on each of the nodes to withstand network failures or node crashes.

This implementation doesn't have any networking support and communicates via stdin/stdout (since its a toy implementation). It is designed and tested using the amazing [jepsen-io/Maelstrom](https://github.com/jepsen-io/maelstrom) framework.

I've tried to stick pretty close to [maelstrom/doc/06-raft](https://github.com/jepsen-io/maelstrom/tree/main/doc/06-raft) in terms of the core logic but the implementation remains in Rust instead of Ruby.

## Status
- [x] [01-key-value](https://github.com/jepsen-io/maelstrom/blob/main/doc/06-raft/01-key-value.md): Set up RPC handlers for the simple lin-kv workload.
- [x] [02-leader-election](https://github.com/jepsen-io/maelstrom/blob/main/doc/06-raft/02-leader-election.md): Implement the leader election related RPCs and set up the state transitions for the election process.
- [ ] [03-replication](https://github.com/jepsen-io/maelstrom/blob/main/doc/06-raft/03-replication.md): Set up leader nodes to replicate logs to followers but without fiddling with commit indexes.
- [ ] [04-committing](https://github.com/jepsen-io/maelstrom/blob/main/doc/06-raft/04-committing.md): Integrate commit indexes and offsets to the replication process.

## Build

Pretty straightforward using `cargo`:
```sh
cargo build --release
# generates a raft binary: target/release/raft
```

## Usage

### Setup

This repo ships a submodule of maelstrom so we just need to sync the repo:
```sh
git submodule sync --recursive
# Use the executable: maelstrom/maelstrom 
# to execute any maelstrom commands mentioned here.
```
In case of any errors setting up maelstrom, [this](https://github.com/jepsen-io/maelstrom/blob/main/doc/01-getting-ready/index.md) is an excellent guide to help you set it up correctly.

### Raft

Not only are we implementing the Raft protocol, we'll be using the protocol to drive a linearizable key-value store. One could think of Raft living in the implementation details of some key-value store like Redis. For example, [Hashicorp Consul](https://www.consul.io/), [Apache Zookeeper](https://zookeeper.apache.org/), [etcd](https://etcd.io/) all use some sort of Raft implementation to facilitate log replication via consensus.

The system we implement here is quite basic and only respects the `read`, `write`, `compare-and-swap (cas)` RPCs as described in the following:

```clojure
{"type"       "read"
 "msg_id"     An integer
 "key"        A string: the key the client would like to read}

{"type"     "write"
 "msg_id"   An integer
 "key"      A string: the key the client would like to write
 "value"    A string: the value the client would like to write}

{"type"     "cas"
 "msg_id"   An integer
 "key"      A string: the key the client would like to write
 "from"     A string: the value that the client expects to be present
 "to"       A string: the value to write if and only if the value is `from`}
```

Since there is no actual networking layer in this implementation, we must provide it a runtime
that collects outbound messages for different nodes via `stdout` and relays any incoming messages via `stdin` of a process that represents a single node in the cluster.

Alternatively, we can run it under maelstrom's supervision with:
```sh
RUST_LOG=info maelstrom test \
    -w lin-kv \
    --bin target/release/raft \
    --time-limit 10 \
    --node-count 3 \
    --rate 10 \
    --concurrency 2n
```

Once the workload completes, we can serve up the analysis and logs with
```sh
maelstrom serve
```

Here's what some of the logs could look like:

```
2023-04-12T02:16:09.459Z INFO  [election] Just reset the election deadline to 2023-04-12 02:16:12.339571817 UTC
2023-04-12T02:16:09.459Z INFO  [election] Became candidate for term: 1
2023-04-12T02:16:09.459Z INFO  [election] About to request for votes to our peers (["n1", "n0"])
2023-04-12T02:16:09.460Z INFO  [raft-rpc] Received: RequestVoteOk { term: 1, vote_granted: true }

2023-04-12T02:16:09.460Z INFO  [election] Have votes: {"n1"}
2023-04-12T02:16:09.460Z INFO  [raft-rpc] Received: RequestVoteOk { term: 1, vote_granted: true }

2023-04-12T02:16:09.460Z INFO  [election] Have votes: {"n1", "n0"}
2023-04-12T02:16:09.460Z INFO  [election] Became a leader for term: 1
2023-04-12T02:16:11.560Z INFO  [election] Stepping down: haven't received any acks recently.
2023-04-12T02:16:11.560Z INFO  [election] Became follower for term: 1
2023-04-12T02:16:11.560Z INFO  [election] Just reset the election deadline to 2023-04-12 02:16:13.838792571 UTC
2023-04-12T02:16:12.932Z INFO  [raft-rpc] Received: RequestVote { term: 2, candidate_id: "n1", last_log_index: 1, last_log_term: 0 }

2023-04-12T02:16:12.932Z INFO  [election] Stepping down because remote term (2) is higher than our term (1)
2023-04-12T02:16:12.932Z INFO  [election] Became follower for term: 2
2023-04-12T02:16:12.932Z INFO  [election] Just reset the election deadline to 2023-04-12 02:16:15.587167750 UTC
2023-04-12T02:16:12.932Z INFO  [raft-rpc] Granting vote to n1
2023-04-12T02:16:12.932Z INFO  [election] Just reset the election deadline to 2023-04-12 02:16:15.714044090 UTC
2023-04-12T02:16:15.753Z INFO  [election] Just reset the election deadline to 2023-04-12 02:16:18.463370072 UTC
2023-04-12T02:16:15.753Z INFO  [election] Became candidate for term: 3
2023-04-12T02:16:15.753Z INFO  [election] About to request for votes to our peers (["n1", "n0"])
2023-04-12T02:16:15.753Z INFO  [raft-rpc] Received: RequestVoteOk { term: 3, vote_granted: true }

2023-04-12T02:16:15.753Z INFO  [election] Have votes: {"n1", "n0"}
2023-04-12T02:16:15.753Z INFO  [election] Became a leader for term: 3
2023-04-12T02:16:15.754Z INFO  [raft-rpc] Received: RequestVoteOk { term: 3, vote_granted: true }

2023-04-12T02:16:17.763Z INFO  [election] Stepping down: haven't received any acks recently.
2023-04-12T02:16:17.763Z INFO  [election] Became follower for term: 3
2023-04-12T02:16:17.763Z INFO  [election] Just reset the election deadline to 2023-04-12 02:16:21.384772084 UTC
2023-04-12T02:16:17.948Z INFO  [raft-rpc] Received: RequestVote { term: 4, candidate_id: "n1", last_log_index: 1, last_log_term: 0 }

2023-04-12T02:16:17.948Z INFO  [election] Stepping down because remote term (4) is higher than our term (3)
2023-04-12T02:16:17.948Z INFO  [election] Became follower for term: 4
2023-04-12T02:16:17.948Z INFO  [election] Just reset the election deadline to 2023-04-12 02:16:20.138138548 UTC
2023-04-12T02:16:17.948Z INFO  [raft-rpc] Granting vote to n1
2023-04-12T02:16:17.948Z INFO  [election] Just reset the election deadline to 2023-04-12 02:16:21.623223464 UTC
```

### Conclusion

We'd say the implementation is correct if it successfully passes the `lin-kv` workload in the most aggresive of tests. The difficulty of tests can be tuned by passing appropriate flags to the maelstrom executable.

Since this implementation isn't fully complete yet, we don't expect it to distribute at all and therefore fail most distributed tests. I'm mostly generating logs from the runs and trying to ensure the invariants such as:
- only grant votes for higher or equal terms,
- don't vote twice in the same term,
- candidate's log is at least as big as ours,
- etc.

My medium-term goal is to complete the full implementation and pass all maelstrom tests for the `lin-kv` workload.

## Contributing

[Issues](https://github.com/aalekhpatel07/maelstrom-raft/issues/new), [pull requests](https://github.com/aalekhpatel07/maelstrom-raft/pulls) are always welcome.

## Support
If you found this cool or liked reading some of it, I'll always appreciate ~~programmer clout~~ [Github stars](https://github.com/aalekhpatel07/maelstrom-raft).