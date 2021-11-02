// A rate limiter with priorities.
// Piggybacks off of leaky_bucket and flume.
// This file is ripe for code deduplication.

use flume::{bounded, Receiver, Sender};
use futures::select_biased;
use futures::FutureExt;
use leaky_bucket::RateLimiter;
use std::time::Duration;

#[derive(Debug)]
pub struct PrioLimit {
    /// Priority 1 goes to access token requests.
    access_token_sender: Sender<Req>,
    /// Priority 2 goes to regular retries.
    retry_sender: Sender<Req>,
    /// Priority 3 goes to regular requests.
    sender: Sender<Req>,
    /// Priority 4 goes to background retries.
    bg_retry_sender: Sender<Req>,
    /// Priority 5 goes to background requests.
    bg_sender: Sender<Req>,
}

impl PrioLimit {
    // tokens 1, max 10, refill_interval Duration::from_millis(120)
    pub fn new(tokens: usize, max: usize, refill_interval: Duration) -> PrioLimit {
        let (access_token_sender, access_token_recv) = bounded(0);
        let (retry_sender, retry_recv) = bounded(0);
        let (sender, recv) = bounded(0);
        let (bg_retry_sender, bg_retry_recv) = bounded(0);
        let (bg_sender, bg_recv) = bounded(0);

        let leaky_bucket = RateLimiter::builder()
            .initial(tokens)
            .max(max)
            .refill(1)
            .interval(refill_interval)
            .build();

        tokio::spawn(Self::internal(
            leaky_bucket,
            access_token_recv,
            retry_recv,
            recv,
            bg_retry_recv,
            bg_recv,
        ));

        PrioLimit {
            access_token_sender,
            retry_sender,
            sender,
            bg_retry_sender,
            bg_sender,
        }
    }

    /// Wait for a access token request to be available.
    pub async fn access_token_wait(&self) {
        let (send, recv) = bounded(0);
        self.access_token_sender
            .send_async(Req { responder: send })
            .await
            .unwrap();

        recv.recv_async().await.unwrap();
    }

    /// Wait for a regular request to be available.
    pub async fn retry_wait(&self) {
        let (send, recv) = bounded(0);
        self.retry_sender
            .send_async(Req { responder: send })
            .await
            .unwrap();

        recv.recv_async().await.unwrap();
    }

    /// Wait for a retry request to be available.
    pub async fn wait(&self) {
        let (send, recv) = bounded(0);
        self.sender
            .send_async(Req { responder: send })
            .await
            .unwrap();

        recv.recv_async().await.unwrap();
    }

    /// Wait for a background retry request to be available.
    pub async fn bg_retry_wait(&self) {
        let (send, recv) = bounded(0);
        self.bg_retry_sender
            .send_async(Req { responder: send })
            .await
            .unwrap();

        recv.recv_async().await.unwrap();
    }

    /// Wait for a background request to be available.
    pub async fn bg_wait(&self) {
        let (send, recv) = bounded(0);
        self.bg_sender
            .send_async(Req { responder: send })
            .await
            .unwrap();

        recv.recv_async().await.unwrap();
    }

    async fn internal(
        leaky_bucket: RateLimiter,
        access_token_recv: Receiver<Req>,
        retry_recv: Receiver<Req>,
        recv: Receiver<Req>,
        bg_retry_recv: Receiver<Req>,
        bg_recv: Receiver<Req>,
    ) {
        loop {
            let next_req = select_biased! {
                x = access_token_recv.recv_async().fuse() => x,
                x = retry_recv.recv_async().fuse() => x,
                x = recv.recv_async().fuse() => x,
                x = bg_retry_recv.recv_async().fuse() => x,
                x = bg_recv.recv_async().fuse() => x,
            };

            leaky_bucket.acquire_one().await;

            if let Ok(next_req) = next_req {
                next_req
                    .responder
                    .send_async(())
                    .await
                    .expect("Could not wake up rate-limited future");
            } else {
                // PrioLimit has shut down
                break;
            }
        }
    }
}

#[derive(Debug)]
struct Req {
    responder: Sender<()>,
}
