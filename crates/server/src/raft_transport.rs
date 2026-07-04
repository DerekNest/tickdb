/// TCP transport for Raft messages (milestone 9, §4.4).
///
/// # Framing
///
/// Each frame on the wire is: `[4-byte big-endian length][JSON payload]`.
/// JSON (not a binary format) is used deliberately — Raft control traffic
/// is tiny relative to the WAL/segment data it orchestrates, so the
/// simplicity and debuggability of a human-readable wire format costs
/// nothing that matters here.
///
/// # Attribution without a handshake
///
/// A connection's peer identity is never negotiated explicitly — every
/// `raft::Message` variant already carries its sender's `NodeId` in a field
/// (`Message::sender_id()`), so the transport just reads that back out.
/// This keeps the wire protocol to exactly one thing: frames of messages.
///
/// # Chunked snapshot transfer
///
/// `Message::InstallSnapshot.data` can be large (a bundled manifest +
/// segment files, §4.4). Splitting a multi-hundred-MiB blob into one JSON
/// frame would spike memory and head-of-line-block every other message on
/// the same connection. Instead, `WireFrame::SnapshotChunk` carries the
/// snapshot in ≤1MiB pieces; the receiving side reassembles them (buffered
/// per `(leader_id, last_included_index)`) before handing a complete
/// `Message::InstallSnapshot` up to the Raft state machine, which — per its
/// own design (see `raft::types::Message::InstallSnapshot`'s doc comment)
/// — is never aware chunking happened at all.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};

use raft::{LogIndex, Message, NodeId, Term};

/// Snapshot chunks are capped at 1MiB (§4.4).
pub const SNAPSHOT_CHUNK_SIZE: usize = 1024 * 1024;

/// A single wire frame: either a normal Raft message, or one piece of a
/// chunked snapshot transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum WireFrame {
    RaftMessage(Message),
    SnapshotChunk {
        term: Term,
        leader_id: NodeId,
        last_included_index: LogIndex,
        last_included_term: Term,
        chunk_index: u32,
        total_chunks: u32,
        bytes: Vec<u8>,
    },
}

/// Reads one length-prefixed frame from `stream`. Returns `Ok(None)` on
/// clean EOF (peer closed the connection).
async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Option<WireFrame>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;

    let frame: WireFrame = serde_json::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(frame))
}

/// Writes one length-prefixed frame to `stream`.
async fn write_frame(stream: &mut TcpStream, frame: &WireFrame) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(frame)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let len = (bytes.len() as u32).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(&bytes).await?;
    stream.flush().await
}

/// Splits `data` into ≤1MiB `SnapshotChunk` frames and writes them in order.
///
/// Only used directly by tests (which write straight to a `TcpStream`);
/// the production send path (`PeerConnections::send_maybe_chunked`) does
/// the same splitting inline against the mpsc-queued writer instead, since
/// it can't borrow a `TcpStream` directly (the writer task owns it).
#[cfg(test)]
async fn write_snapshot_chunked(
    stream: &mut TcpStream,
    term: Term,
    leader_id: NodeId,
    last_included_index: LogIndex,
    last_included_term: Term,
    data: &[u8],
) -> std::io::Result<()> {
    let total_chunks = data.chunks(SNAPSHOT_CHUNK_SIZE).count().max(1) as u32;
    if data.is_empty() {
        // Still send exactly one (empty) chunk so the receiver's
        // reassembly logic — which waits for `total_chunks` pieces — has
        // something to wait for and complete on.
        let frame = WireFrame::SnapshotChunk {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            chunk_index: 0,
            total_chunks: 1,
            bytes: Vec::new(),
        };
        return write_frame(stream, &frame).await;
    }
    for (i, chunk) in data.chunks(SNAPSHOT_CHUNK_SIZE).enumerate() {
        let frame = WireFrame::SnapshotChunk {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            chunk_index: i as u32,
            total_chunks,
            bytes: chunk.to_vec(),
        };
        write_frame(stream, &frame).await?;
    }
    Ok(())
}

// ── Inbound side ────────────────────────────────────────────────────────────

/// Key identifying one in-progress snapshot reassembly.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SnapshotKey {
    leader_id: NodeId,
    last_included_index: LogIndex,
}

/// Buffers partial snapshot chunks until a complete transfer arrives.
#[derive(Default)]
struct SnapshotReassembly {
    partial: HashMap<SnapshotKey, PartialSnapshot>,
}

struct PartialSnapshot {
    term: Term,
    last_included_term: Term,
    chunks: Vec<Option<Vec<u8>>>,
}
struct SnapshotMeta {
    term: Term,
    leader_id: NodeId,
    last_included_index: LogIndex,
    last_included_term: Term,
}

impl SnapshotReassembly {
    /// Feeds one chunk in. Returns `Some(complete_message)` once every
    /// chunk for that transfer has arrived.
    fn feed(
        &mut self,
        meta: SnapshotMeta,
        chunk_index: u32,
        total_chunks: u32,
        bytes: Vec<u8>,
    ) -> Option<Message> {
        let key = SnapshotKey {
            leader_id: meta.leader_id,
            last_included_index: meta.last_included_index,
        };
        let entry = self
            .partial
            .entry(key.clone())
            .or_insert_with(|| PartialSnapshot {
                term: meta.term,
                last_included_term: meta.last_included_term,
                chunks: vec![None; total_chunks as usize],
            });

        if (chunk_index as usize) < entry.chunks.len() {
            entry.chunks[chunk_index as usize] = Some(bytes);
        }

        if entry.chunks.iter().all(|c| c.is_some()) {
            let entry = self.partial.remove(&key).unwrap();
            let data: Vec<u8> = entry.chunks.into_iter().flatten().flatten().collect();
            return Some(Message::InstallSnapshot {
                term: entry.term,
                leader_id: key.leader_id,
                last_included_index: key.last_included_index,
                last_included_term: entry.last_included_term,
                data,
            });
        }

        None
    }
}

/// Spawns a TCP listener on `bind_addr`. Every complete `raft::Message`
/// received on any connection (after snapshot reassembly, if applicable)
/// is forwarded as `(sender_id, message)` on `inbound_tx`.
///
/// Runs until the process exits — there's no clean shutdown handle because
/// the server is expected to run for its whole lifetime; tests that need to
/// stop early just drop the listening task's `JoinHandle`.
pub fn spawn_listener(
    bind_addr: SocketAddr,
    inbound_tx: mpsc::Sender<(NodeId, Message)>,
) -> tokio::task::JoinHandle<std::io::Result<()>> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(bind_addr).await?;
        tracing::info!(%bind_addr, "raft transport listening");

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let tx = inbound_tx.clone();
            tokio::spawn(handle_connection(stream, peer_addr, tx));
        }
    })
}

async fn handle_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    inbound_tx: mpsc::Sender<(NodeId, Message)>,
) {
    let mut reassembly = SnapshotReassembly::default();

    loop {
        match read_frame(&mut stream).await {
            Ok(Some(WireFrame::RaftMessage(msg))) => {
                let sender = msg.sender_id();
                if inbound_tx.send((sender, msg)).await.is_err() {
                    return; // runtime shut down
                }
            }
            Ok(Some(WireFrame::SnapshotChunk {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                chunk_index,
                total_chunks,
                bytes,
            })) => {
                if let Some(complete) = reassembly.feed(
                    SnapshotMeta {
                        term,
                        leader_id,
                        last_included_index,
                        last_included_term,
                    },
                    chunk_index,
                    total_chunks,
                    bytes,
                ) {
                    if inbound_tx.send((leader_id, complete)).await.is_err() {
                        return;
                    }
                }
            }
            Ok(None) => {
                tracing::debug!(%peer_addr, "raft transport connection closed");
                return;
            }
            Err(e) => {
                tracing::warn!(%peer_addr, err = %e, "raft transport read error");
                return;
            }
        }
    }
}

// ── Outbound side ────────────────────────────────────────────────────────────

/// One outbound connection slot: a queue of messages to send, drained by a
/// dedicated writer task that reconnects on failure.
///
/// Reconnection is deliberately best-effort and lossy: if the peer is
/// unreachable, queued messages are simply dropped rather than buffered
/// indefinitely. This matches Raft's own design — the protocol already
/// tolerates arbitrary message loss (heartbeats and retries paper over
/// gaps), so an unbounded retry queue would only add complexity without
/// adding correctness.
struct PeerConnection {
    tx: mpsc::Sender<WireFrame>,
}

/// Manages outbound connections to every peer in the cluster.
pub struct PeerConnections {
    peers: Arc<Mutex<HashMap<NodeId, PeerConnection>>>,
}

impl PeerConnections {
    /// Spawns one writer task per entry in `addrs` (peer id -> socket
    /// address). Each task lazily connects on first send and reconnects
    /// automatically after a failure.
    pub fn spawn(addrs: HashMap<NodeId, String>) -> Self {
        let mut peers = HashMap::new();
        for (&node_id, addr) in &addrs {
            let (tx, rx) = mpsc::channel(1024);
            tokio::spawn(peer_writer_loop(node_id, addr.clone(), rx));
            peers.insert(node_id, PeerConnection { tx });
        }
        PeerConnections {
            peers: Arc::new(Mutex::new(peers)),
        }
    }

    /// Enqueues `msg` for delivery to `peer`. Non-blocking best-effort: if
    /// the peer's queue is full (very slow/stuck connection) or unknown,
    /// the message is silently dropped — same rationale as connection loss.
    pub async fn send(&self, peer: NodeId, msg: Message) {
        let peers = self.peers.lock().await;
        if let Some(conn) = peers.get(&peer) {
            let _ = conn.tx.try_send(WireFrame::RaftMessage(msg));
        }
    }

    /// Like `send`, but for an `InstallSnapshot` message specifically —
    /// splits it into chunks (§4.4) instead of one giant frame. Detected by
    /// pattern-matching `msg`; callers don't need to know the difference.
    pub async fn send_maybe_chunked(&self, peer: NodeId, msg: Message) {
        if let Message::InstallSnapshot {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            data,
        } = &msg
        {
            let peers = self.peers.lock().await;
            if let Some(conn) = peers.get(&peer) {
                let chunk_count = data.chunks(SNAPSHOT_CHUNK_SIZE).count().max(1);
                let mut chunks_iter = data.chunks(SNAPSHOT_CHUNK_SIZE);
                for i in 0..chunk_count {
                    let chunk = chunks_iter.next().unwrap_or(&[]);
                    let frame = WireFrame::SnapshotChunk {
                        term: *term,
                        leader_id: *leader_id,
                        last_included_index: *last_included_index,
                        last_included_term: *last_included_term,
                        chunk_index: i as u32,
                        total_chunks: chunk_count as u32,
                        bytes: chunk.to_vec(),
                    };
                    let _ = conn.tx.try_send(frame);
                }
            }
            return;
        }
        self.send(peer, msg).await;
    }
}

async fn peer_writer_loop(node_id: NodeId, addr: String, mut rx: mpsc::Receiver<WireFrame>) {
    loop {
        let stream = match TcpStream::connect(addr.clone()).await {
            Ok(s) => s,
            Err(e) => {
                tracing::debug!(peer = node_id, %addr, err = %e, "raft transport: connect failed, retrying");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            }
        };
        let mut stream = stream;

        // Drain the queue onto this connection until a write fails, then
        // reconnect. Messages already sent before the failure aren't
        // re-sent — see the struct doc comment on why this is fine for Raft.
        loop {
            let Some(frame) = rx.recv().await else { return }; // channel closed: shutting down
            if let Err(e) = write_frame(&mut stream, &frame).await {
                tracing::debug!(peer = node_id, %addr, err = %e, "raft transport: write failed, reconnecting");
                break;
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use raft::Message;

    #[tokio::test]
    async fn frame_roundtrip_over_real_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_frame(&mut stream).await.unwrap().unwrap()
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let msg = Message::RequestVote {
            term: 3,
            candidate_id: 1,
            last_log_index: 5,
            last_log_term: 2,
        };
        write_frame(&mut client, &WireFrame::RaftMessage(msg.clone()))
            .await
            .unwrap();

        let received = server.await.unwrap();
        match received {
            WireFrame::RaftMessage(m) => assert_eq!(m, msg),
            _ => panic!("expected RaftMessage frame"),
        }
    }

    #[tokio::test]
    async fn listener_attributes_message_by_sender_id_not_connection() {
        let (tx, mut rx) = mpsc::channel(16);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            handle_connection(stream, peer_addr, tx).await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        // sender_id() for RequestVote is candidate_id = 7, regardless of
        // which physical connection/port it arrived on.
        let msg = Message::RequestVote {
            term: 1,
            candidate_id: 7,
            last_log_index: 0,
            last_log_term: 0,
        };
        write_frame(&mut client, &WireFrame::RaftMessage(msg))
            .await
            .unwrap();

        let (sender, _msg) = rx.recv().await.unwrap();
        assert_eq!(sender, 7);
    }

    #[tokio::test]
    async fn chunked_snapshot_reassembles_correctly() {
        let (tx, mut rx) = mpsc::channel(16);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            handle_connection(stream, peer_addr, tx).await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        // 3.5 chunks worth of data, to exercise a non-exact-multiple split.
        let data: Vec<u8> = (0..(SNAPSHOT_CHUNK_SIZE * 3 + SNAPSHOT_CHUNK_SIZE / 2))
            .map(|i| (i % 256) as u8)
            .collect();

        write_snapshot_chunked(&mut client, 9, 2, 100, 8, &data)
            .await
            .unwrap();

        let (sender, msg) = rx.recv().await.unwrap();
        assert_eq!(sender, 2);
        match msg {
            Message::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                data: received,
            } => {
                assert_eq!(term, 9);
                assert_eq!(leader_id, 2);
                assert_eq!(last_included_index, 100);
                assert_eq!(last_included_term, 8);
                assert_eq!(
                    received, data,
                    "reassembled snapshot bytes must match the original exactly"
                );
            }
            other => panic!("expected InstallSnapshot, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn empty_snapshot_reassembles_as_single_chunk() {
        let (tx, mut rx) = mpsc::channel(16);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            handle_connection(stream, peer_addr, tx).await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        write_snapshot_chunked(&mut client, 1, 0, 5, 1, &[])
            .await
            .unwrap();

        let (_sender, msg) = rx.recv().await.unwrap();
        match msg {
            Message::InstallSnapshot { data, .. } => assert!(data.is_empty()),
            other => panic!("expected InstallSnapshot, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn peer_connections_deliver_across_real_sockets() {
        // Two real listeners, each wired up as the other's "peer" — sends
        // a message end-to-end through PeerConnections + spawn_listener,
        // exactly as two real server processes would communicate.
        let (tx_a, mut rx_a) = mpsc::channel(16);
        let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_a = listener_a.local_addr().unwrap();
        drop(listener_a);
        spawn_listener(addr_a, tx_a);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await; // let it bind

        let mut addrs = HashMap::new();
        addrs.insert(0u32, addr_a.to_string());
        let conns = PeerConnections::spawn(addrs);

        let msg = Message::AppendEntries {
            term: 1,
            leader_id: 5,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        conns.send(0, msg.clone()).await;

        let (sender, received) =
            tokio::time::timeout(std::time::Duration::from_secs(2), rx_a.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(sender, 5);
        assert_eq!(received, msg);
    }
}
