use crate::{
    service::{
        session_manager_client::SessionManagerClient, GetSessionRequest, GetSessionResponse,
    },
    BoxError,
};

pub async fn get_publish_url(stream_key: &str) -> Result<Option<String>, BoxError> {
    let session_manager_service_url = dotenv::var("SESSION_MANAGER_SERVICE_URL")?;
    let session_manager_service_url = format!("http://{}", session_manager_service_url);
    let mut client = SessionManagerClient::connect(session_manager_service_url).await?;

    let res: GetSessionResponse = client
        .get_session(GetSessionRequest {
            stream_key: stream_key.to_string(),
        })
        .await?
        .into_inner();

    match res.status {
        0 => Ok(Some(res.stream_path)),
        1 => Ok(None),
        _ => Err("Unknown status".into()),
    }
}
