use neo4rs::{Graph, Query, ConfigBuilder, Node, Relation, query};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use log::{debug, error};

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub entity: String,
    pub properties: Vec<Property>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Property {
    pub name: String,
    pub value: String,
}

#[derive(Error, Debug)]
pub enum Neo4jClientError {
    #[error("Neo4j error: {0}")]
    Neo4jError(#[from] neo4rs::Error),
    #[error("Other error: {0}")]
    OtherError(String),
    #[error("Deserialization error: {0}")]
    DeserializationError(#[from] serde_json::Error),
}

pub struct Neo4jClient {
    graph: Graph,
}

impl Neo4jClient {
    pub async fn new(uri: &str, user: &str, password: &str, database: &str) -> Result<Self, Neo4jClientError> {
        let config = ConfigBuilder::default()
            .uri(uri)
            .user(user)
            .password(password)
            .db(database)
            .build()
            .map_err(Neo4jClientError::Neo4jError)?;
        let graph = Graph::connect(config).await?;
        Ok(Neo4jClient { graph })
    }

    pub async fn query_nodes(&self) -> Result<Vec<QueryResult>, Neo4jClientError> {
        let query_str = "MATCH (n) RETURN n";
        let mut result = self.graph.execute(Query::new(query_str.to_string())).await?;
        let mut query_results = Vec::new();

        while let Ok(Some(row)) = result.next().await {
            let node: Node = row.get("n").map_err(|e| Neo4jClientError::OtherError(e.to_string()))?;
            let mut props = Vec::new();
            for key in node.keys() {
                match node.get::<String>(key) {
                    Ok(value) => {
                        props.push(Property {
                            name: key.to_string(),
                            value,
                        });
                    }
                    Err(_) => continue,
                }
            }
            query_results.push(QueryResult {
                entity: format!("Node({})", node.id()),
                properties: props,
            });
        }
        Ok(query_results)
    }

    pub async fn query_relationships(&self) -> Result<Vec<QueryResult>, Neo4jClientError> {
        let query_str = "MATCH ()-[r]->() RETURN r";
        let mut result = self.graph.execute(Query::new(query_str.to_string())).await?;
        let mut query_results = Vec::new();

        while let Ok(Some(row)) = result.next().await {
            let relationship: Relation = row.get("r").map_err(|e| Neo4jClientError::OtherError(e.to_string()))?;
            let mut props = Vec::new();
            for key in relationship.keys() {
                match relationship.get::<String>(key) {
                    Ok(value) => {
                        props.push(Property {
                            name: key.to_string(),
                            value,
                        });
                    }
                    Err(_) => continue,
                }
            }
            query_results.push(QueryResult {
                entity: format!("Relationship({})", relationship.id()),
                properties: props,
            });
        }
        Ok(query_results)
    }

    pub async fn check_node_exists(&self, node_id: i64) -> Result<bool, Neo4jClientError> {
        let query_str = format!("MATCH (n) WHERE ID(n) = {} RETURN n", node_id);
        let q = Query::new(query_str.clone());
        debug!("Check node existence Query_string: {}", query_str);

        let mut result = self.graph.execute(q).await?;
        Ok(result.next().await?.is_some())
    }

    pub async fn create_relationship(&self, start_id: i64, end_id: i64, rel_type: &str) -> Result<(), Neo4jClientError> {
        // Check if the relationship already exists
        let check_query_str = format!(
            "MATCH (a)-[r:{}]->(b) WHERE ID(a) = {} AND ID(b) = {} RETURN r",
            rel_type, start_id, end_id
        );
        let check_q = query(&check_query_str);
        debug!("Check relationship existence Query_string: {}", check_query_str);

        let mut check_result = self.graph.execute(check_q).await?;
        if check_result.next().await?.is_some() {
            debug!("Relationship between {} and {} already exists", start_id, end_id);
            return Ok(());
        }

        // Create the relationship
        let create_query_str = format!(
            "MATCH (a), (b) WHERE ID(a) = {} AND ID(b) = {} CREATE (a)-[:{}]->(b)",
            start_id, end_id, rel_type
        );
        let create_q = query(&create_query_str);
        debug!("Create relationship Query_string: {}", create_query_str);

        match self.graph.run(create_q).await {
            Ok(_) => {
                debug!("Successfully created relationship between {} and {}", start_id, end_id);
            },
            Err(e) => {
                error!("Failed to create relationship between {} and {}: {}", start_id, end_id, e);
                return Err(Neo4jClientError::Neo4jError(e));
            }
        }

        Ok(())
    }


    pub async fn get_internal_node_id_by_content(&self, content: &str) -> Result<Option<i64>, Neo4jClientError> {
        let query_str = "MATCH (n {content: $content}) RETURN ID(n)".to_string();
        let q = Query::new(query_str.clone()).param("content", content.to_string());
        debug!("Get internal node ID by content Query: {}, content: {}", query_str, content);

        let mut result = self.graph.execute(q).await?;
        if let Some(row) = result.next().await? {
            let node_id: i64 = row.get("ID(n)").map_err(|e| Neo4jClientError::OtherError(e.to_string()))?;
            debug!("Internal Node ID by content: {}", node_id);
            return Ok(Some(node_id));
        }

        Ok(None)
    }

    pub async fn get_internal_node_id(&self, id: &str) -> Result<Option<i64>, Neo4jClientError> {
        let query_str: String;
        let q: Query;

        if id.chars().all(char::is_numeric) {
            query_str = "MATCH (n) WHERE ID(n) = $id RETURN ID(n)".to_string();
            q = Query::new(query_str.clone()).param("id", id.parse::<i64>().map_err(|e| Neo4jClientError::OtherError(e.to_string()))?);
            debug!("Get internal node ID by internal ID Query: {}, id: {}", query_str, id);
        } else {
            query_str = "MATCH (n {id: $external_id}) RETURN ID(n)".to_string();
            q = Query::new(query_str.clone()).param("external_id", id.to_string());
            debug!("Get internal node ID by external ID Query: {}, external_id: {}", query_str, id);
        }

        let mut result = self.graph.execute(q).await?;
        if let Some(row) = result.next().await? {
            let node_id: i64 = row.get("ID(n)").map_err(|e| Neo4jClientError::OtherError(e.to_string()))?;
            debug!("Internal Node ID: {}", node_id);
            return Ok(Some(node_id));
        }

        Ok(None)
    }
}