use std::fmt::Debug;
use std::sync::Arc;

use arrow_pg::datatypes::df::encode_dataframe;
use async_trait::async_trait;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use futures::Sink;
use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::Format;
use pgwire::api::query::{SimpleQueryHandler, send_execution_response, send_query_response};
use pgwire::api::results::{Response, Tag};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::error::{not_supported, to_pg_error};

/// Serves one client connection. Owns that connection's DataFusion session, so session state
/// (settings, temporary tables) is never shared between connections.
pub(crate) struct ConnectionHandler {
    ctx: SessionContext,
}

impl ConnectionHandler {
    pub(crate) fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }

    /// Runs one statement to completion and sends its result to the client, so the next statement
    /// of the query observes its effects.
    async fn execute<C>(&self, client: &mut C, statement: Statement) -> PgWireResult<()>
    where
        C: Sink<PgWireBackendMessage> + Unpin,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let plan = self
            .ctx
            .state()
            .statement_to_plan(statement)
            .await
            .map_err(to_pg_error)?;
        reject_unsupported(&plan)?;
        let df = self
            .ctx
            .execute_logical_plan(plan)
            .await
            .map_err(to_pg_error)?;
        // Statements without output columns (DDL, SET) have already run inside
        // `execute_logical_plan`.
        if df.schema().fields().is_empty() {
            return send_execution_response(client, Tag::new("OK")).await;
        }
        // Sending drains the DataFrame's stream, which is what executes DML and queries.
        let response = encode_dataframe(df, &Format::UnifiedText, None).await?;
        send_query_response(client, response, true).await
    }
}

/// Rejects plans that read or write arbitrary local files, anywhere in the plan (including under
/// `EXPLAIN ANALYZE`, which executes its input). Tables come from Iceberg catalogs instead.
fn reject_unsupported(plan: &LogicalPlan) -> PgWireResult<()> {
    let mut unsupported = None;
    plan.apply(|node| {
        unsupported = match node {
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(_)) => Some("CREATE EXTERNAL TABLE"),
            LogicalPlan::Copy(_) => Some("COPY"),
            _ => None,
        };
        Ok(if unsupported.is_some() {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })
    .map_err(to_pg_error)?;
    match unsupported {
        Some(feature) => Err(not_supported(feature)),
        None => Ok(()),
    }
}

// Authentication is added in M4; until then every startup is accepted.
impl NoopStartupHandler for ConnectionHandler {}

#[async_trait]
impl SimpleQueryHandler for ConnectionHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let statements =
            DFParser::parse_sql_with_dialect(query, &PostgreSqlDialect {}).map_err(to_pg_error)?;
        if statements.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }
        // Each statement's result is sent before the next statement is planned. On error the
        // results already sent stay with the client, pgwire follows them with the ErrorResponse
        // and ReadyForQuery, and the remaining statements are skipped, as in PostgreSQL.
        for statement in statements {
            self.execute(client, statement).await?;
        }
        Ok(Vec::new())
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
