use datafusion::error::DataFusionError;
use pgwire::error::{ErrorInfo, PgWireError};

/// SQLSTATE `42601`: syntax error.
pub const SYNTAX_ERROR: &str = "42601";
/// SQLSTATE `XX000`: internal error.
pub const INTERNAL_ERROR: &str = "XX000";

/// Maps a DataFusion error to the SQLSTATE reported to the client.
pub fn sqlstate(error: &DataFusionError) -> &'static str {
    match error.find_root() {
        DataFusionError::SQL(..) => SYNTAX_ERROR,
        _ => INTERNAL_ERROR,
    }
}

/// Converts a DataFusion error into an `ERROR` response for the client.
pub fn to_pg_error(error: DataFusionError) -> PgWireError {
    let code = sqlstate(&error);
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        error.to_string(),
    )))
}

#[cfg(test)]
mod tests {
    use datafusion::sql::parser::DFParser;

    use super::*;

    fn parse_error() -> DataFusionError {
        DFParser::parse_sql("SELEC 1").unwrap_err()
    }

    #[test]
    fn syntax_error_maps_to_42601() {
        // Arrange
        let error = parse_error();

        // Act
        let code = sqlstate(&error);

        // Assert
        assert_eq!(code, SYNTAX_ERROR);
    }

    #[test]
    fn wrapped_syntax_error_maps_to_42601() {
        // Arrange
        let error = DataFusionError::Context("while parsing".to_owned(), Box::new(parse_error()));

        // Act
        let code = sqlstate(&error);

        // Assert
        assert_eq!(code, SYNTAX_ERROR);
    }

    #[test]
    fn other_errors_map_to_internal_error() {
        // Arrange
        let error = DataFusionError::Plan("table 't' not found".to_owned());

        // Act
        let code = sqlstate(&error);

        // Assert
        assert_eq!(code, INTERNAL_ERROR);
    }

    #[test]
    fn to_pg_error_builds_user_error() {
        // Arrange
        let error = DataFusionError::Execution("boom".to_owned());

        // Act
        let pg_error = to_pg_error(error);

        // Assert
        match pg_error {
            PgWireError::UserError(info) => {
                assert_eq!(info.severity, "ERROR");
                assert_eq!(info.code, INTERNAL_ERROR);
                assert!(info.message.contains("boom"), "{}", info.message);
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
