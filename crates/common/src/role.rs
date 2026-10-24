use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// The role a process plays in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Accepts client connections, plans queries and coordinates workers.
    Coordinator,
    /// Executes query stages on behalf of coordinators.
    Worker,
    /// Coordinator and worker in one process (development, single node).
    All,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Coordinator => "coordinator",
            Role::Worker => "worker",
            Role::All => "all",
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Returned when parsing a string that is not a known [`Role`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown role `{0}`; expected one of: coordinator, worker, all")]
pub struct UnknownRole(pub String);

impl FromStr for Role {
    type Err = UnknownRole;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "coordinator" => Ok(Role::Coordinator),
            "worker" => Ok(Role::Worker),
            "all" => Ok(Role::All),
            other => Err(UnknownRole(other.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_roles() {
        // Arrange
        let inputs = ["coordinator", "worker", "all"];

        // Act
        let parsed: Vec<Role> = inputs.iter().map(|s| s.parse().unwrap()).collect();

        // Assert
        assert_eq!(parsed, vec![Role::Coordinator, Role::Worker, Role::All]);
    }

    #[test]
    fn display_round_trips_through_from_str() {
        // Arrange
        let roles = [Role::Coordinator, Role::Worker, Role::All];

        // Act
        let round_tripped: Vec<Role> = roles
            .iter()
            .map(|r| r.to_string().parse().unwrap())
            .collect();

        // Assert
        assert_eq!(round_tripped, roles);
    }

    #[test]
    fn rejects_unknown_role() {
        // Arrange
        let input = "leader";

        // Act
        let err = input.parse::<Role>().unwrap_err();

        // Assert
        assert_eq!(err, UnknownRole("leader".to_owned()));
        assert_eq!(
            err.to_string(),
            "unknown role `leader`; expected one of: coordinator, worker, all"
        );
    }
}
