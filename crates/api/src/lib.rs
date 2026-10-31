use std::{
    collections::HashMap,
    error::Error,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use backtest_dataset::{
    ColumnRole, DatasetError, DatasetReport, DatasetWarning, inspect_signals_csv,
};
use backtest_engine_adapter::BacktestResult;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub struct ApiState {
    jobs: Arc<Mutex<HashMap<u64, BacktestJob>>>,
    next_id: Arc<AtomicU64>,
}

impl Default for ApiState {
    fn default() -> Self {
        Self {
            jobs: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }
}

pub fn app() -> Router {
    app_with_state(ApiState::default())
}

pub fn app_with_state(state: ApiState) -> Router {
    Router::new()
        .route("/datasets/inspect", post(inspect_dataset))
        .route("/backtests", post(create_backtest))
        .route("/backtests/{id}", get(get_backtest))
        .with_state(state)
}

#[derive(Clone, Debug, Serialize)]
pub struct DatasetReportResponse {
    pub schema: Vec<ColumnResponse>,
    pub row_count: usize,
    pub signal_count: usize,
    pub feature_count: usize,
    pub warnings: Vec<DatasetWarningResponse>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ColumnResponse {
    pub name: String,
    pub role: &'static str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DatasetWarningResponse {
    MissingOptionalColumn {
        column: &'static str,
    },
    EmptyFeatureColumn {
        column: String,
    },
    SparseFeatureColumn {
        column: String,
        present: usize,
        total: usize,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BacktestStatus {
    Completed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BacktestJob {
    pub id: u64,
    pub status: BacktestStatus,
    pub result: BacktestResultResponse,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BacktestResultResponse {
    pub order_count: usize,
    pub fill_count: usize,
    pub final_equity: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct CreateBacktestResponse {
    pub id: u64,
    pub status: BacktestStatus,
}

#[derive(Clone, Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

async fn inspect_dataset(body: Bytes) -> Result<axum::Json<DatasetReportResponse>, ApiError> {
    let report = inspect_signals_csv(body.as_ref())?;
    Ok(axum::Json(report.into()))
}

async fn create_backtest(State(state): State<ApiState>) -> axum::Json<CreateBacktestResponse> {
    let id = state.next_id.fetch_add(1, Ordering::Relaxed);
    let job = BacktestJob {
        id,
        status: BacktestStatus::Completed,
        result: BacktestResultResponse::from(BacktestResult {
            orders: Vec::new(),
            fills: Vec::new(),
            final_equity: 0.0,
        }),
    };
    let status = job.status.clone();
    state.jobs.lock().await.insert(id, job);
    axum::Json(CreateBacktestResponse { id, status })
}

async fn get_backtest(
    State(state): State<ApiState>,
    Path(id): Path<u64>,
) -> Result<axum::Json<BacktestJob>, ApiError> {
    state
        .jobs
        .lock()
        .await
        .get(&id)
        .cloned()
        .map(axum::Json)
        .ok_or(ApiError::NotFound(id))
}

impl From<DatasetReport> for DatasetReportResponse {
    fn from(report: DatasetReport) -> Self {
        Self {
            schema: report
                .schema
                .columns
                .into_iter()
                .map(|column| ColumnResponse {
                    name: column.name,
                    role: role_name(column.role),
                })
                .collect(),
            row_count: report.row_count,
            signal_count: report.signal_count,
            feature_count: report.feature_count,
            warnings: report.warnings.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<DatasetWarning> for DatasetWarningResponse {
    fn from(warning: DatasetWarning) -> Self {
        match warning {
            DatasetWarning::MissingOptionalColumn(column) => Self::MissingOptionalColumn { column },
            DatasetWarning::EmptyFeatureColumn { column } => Self::EmptyFeatureColumn { column },
            DatasetWarning::SparseFeatureColumn {
                column,
                present,
                total,
            } => Self::SparseFeatureColumn {
                column,
                present,
                total,
            },
        }
    }
}

impl From<BacktestResult> for BacktestResultResponse {
    fn from(result: BacktestResult) -> Self {
        Self {
            order_count: result.orders.len(),
            fill_count: result.fills.len(),
            final_equity: result.final_equity,
        }
    }
}

const fn role_name(role: ColumnRole) -> &'static str {
    match role {
        ColumnRole::Timestamp => "timestamp",
        ColumnRole::AvailableAt => "available_at",
        ColumnRole::Symbol => "symbol",
        ColumnRole::Signal => "signal",
        ColumnRole::Confidence => "confidence",
        ColumnRole::TargetPosition => "target_position",
        ColumnRole::ValidUntil => "valid_until",
        ColumnRole::Feature => "feature",
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApiError {
    BadRequest(String),
    NotFound(u64),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadRequest(error) => write!(f, "{error}"),
            Self::NotFound(id) => write!(f, "backtest `{id}` was not found"),
        }
    }
}

impl Error for ApiError {}

impl From<DatasetError> for ApiError {
    fn from(error: DatasetError) -> Self {
        Self::BadRequest(error.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
        };
        (
            status,
            axum::Json(ErrorResponse {
                error: self.to_string(),
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request},
    };
    use serde_json::Value;
    use tower::ServiceExt;

    #[tokio::test]
    async fn inspect_endpoint_returns_report_for_csv_body() {
        let response = app()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/datasets/inspect")
                    .body(Body::from("timestamp,symbol,signal\n10,AAPL.XNAS,1\n"))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("json");
        assert_eq!(body["row_count"], 1);
        assert_eq!(body["signal_count"], 1);
    }

    #[tokio::test]
    async fn create_backtest_returns_id() {
        let response = app()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/backtests")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("json");
        assert_eq!(body["id"], 1);
        assert_eq!(body["status"], "completed");
    }

    #[tokio::test]
    async fn result_endpoint_returns_stored_result() {
        let router = app_with_state(ApiState::default());
        router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/backtests")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("created");

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/backtests/1")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("json");
        assert_eq!(body["status"], "completed");
        assert_eq!(body["result"]["order_count"], 0);
    }
}
