use std::sync::Arc;

use arrow_pg::datatypes::df::encode_dataframe;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::Format;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{Response, Tag};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers};
use pgwire::error::PgWireResult;

use crate::error::to_pg_error;

/// Serves one client connection. Owns that connection's DataFusion session, so session state
/// (settings, temporary tables) is never shared between connections.
pub(crate) struct ConnectionHandler {
    ctx: SessionContext,
}

impl ConnectionHandler {
    pub(crate) fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }

    async fn execute(&self, statement: Statement) -> PgWireResult<Response> {
        let plan = self
            .ctx
            .state()
            .statement_to_plan(statement)
            .await
            .map_err(to_pg_error)?;
        let df = self
            .ctx
            .execute_logical_plan(plan)
            .await
            .map_err(to_pg_error)?;
        // Statements without output columns (DDL, SET) have already run inside
        // `execute_logical_plan`.
        if df.schema().fields().is_empty() {
            return Ok(Response::Execution(Tag::new("OK")));
        }
        let response = encode_dataframe(df, &Format::UnifiedText, None).await?;
        Ok(Response::Query(response))
    }
}

// Authentication is added in M4; until then every startup is accepted.
impl NoopStartupHandler for ConnectionHandler {}

#[async_trait]
impl SimpleQueryHandler for ConnectionHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        let statements =
            DFParser::parse_sql_with_dialect(query, &PostgreSqlDialect {}).map_err(to_pg_error)?;
        if statements.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }
        let mut responses = Vec::with_capacity(statements.len());
        for statement in statements {
            responses.push(self.execute(statement).await?);
        }
        Ok(responses)
    }
}

/// The pgwire handler set for one connection.
pub(crate) struct Handlers {
    connection: Arc<ConnectionHandler>,
}

impl Handlers {
    pub(crate) fn new(connection: ConnectionHandler) -> Self {
        Self {
            connection: Arc::new(connection),
        }
    }
}

impl PgWireServerHandlers for Handlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.connection.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.connection.clone()
    }
}
