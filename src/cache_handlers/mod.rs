use async_trait::async_trait;

#[async_trait]
trait CacheHandler {
    async fn hard_cache_writer();
    async fn reader(); // Also acts as a soft cache writer
}