//! Utilities for iroh.
#![deny(missing_docs, rustdoc::broken_intra_doc_links)]

pub mod access_limit;
pub mod connection_pool;

#[cfg(test)]
mod tests {
    use crate::{access_limit::AccessLimit, connection_pool as cp};

    /// Dropping an auto trait from a public type breaks callers, so pin them.
    #[test]
    fn public_types_are_send_sync() {
        fn assert<T: Send + Sync + Unpin + 'static>() {}
        fn access_limit<P: Send + Sync + Unpin + 'static>() {
            assert::<AccessLimit<P>>();
        }

        access_limit::<()>();
        assert::<cp::ConnectionPool>();
        assert::<cp::ConnectionRef>();
        assert::<cp::Options>();
        assert::<cp::PoolConnectError>();
        assert::<cp::ConnectionPoolError>();
    }
}
