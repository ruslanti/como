use anyhow::Result;

struct Client {}

impl Client {
    pub fn new() -> Self {
        Client {}
    }

    async fn connect() -> Result<()> {
        Ok(())
    }

    async fn disconnect() -> Result<()> {
        Ok(())
    }

    async fn publish() -> Result<()> {
        Ok(())
    }

    async fn subscribe() -> Result<()> {
        Ok(())
    }
}
