use neo4rs::{Graph, query, ConfigBuilder, Node, Relation, Query};
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
        let mut result = self.graph.execute(query(query_str)).await?;
        let mut query_results = Vec::new();

        while let Ok(Some(row)) = result.next().await {
            let node: Node = row.get("n").unwrap();
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
        let mut result = self.graph.execute(query(query_str)).await?;
        let mut query_results = Vec::new();

        while let Ok(Some(row)) = result.next().await {
            let relationship: Relation = row.get("r").unwrap();
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

    pub async fn check_node_exists(&self, node_id: &str) -> Result<bool, Neo4jClientError> {
        let query_str = format!("MATCH (n {{id: '{}'}}) RETURN n", node_id);
        let q = query(&query_str);
        debug!("Check node existence Query_string: {}", query_str);

        let mut result = self.graph.execute(q).await?;
        Ok(result.next().await?.is_some())
    }

    pub async fn create_relationship(&self, start_id: &str, end_id: &str, rel_type: &str) -> Result<(), Neo4jClientError> {
        // Check if both nodes exist
        if !self.check_node_exists(start_id).await? {
            error!("Node with id {} does not exist", start_id);
            return Err(Neo4jClientError::OtherError(format!("Node with id {} does not exist", start_id)));
        }

        if !self.check_node_exists(end_id).await? {
            error!("Node with id {} does not exist", end_id);
            return Err(Neo4jClientError::OtherError(format!("Node with id {} does not exist", end_id)));
        }

        // Create the relationship
        let query_str = format!(
            "MATCH (a {{id: '{}'}}), (b {{id: '{}'}}) CREATE (a)-[:{}]->(b)",
            start_id, end_id, rel_type
        );
        let q = query(&query_str);
        debug!("Query_string: {}", query_str);

        match self.graph.run(q).await {
            Ok(_) => {
                debug!("Successfully created relationship between {} and {}", start_id, end_id);
            },
            Err(e) => {
                error!("Failed to create relationship between {} and {}: {}", start_id, end_id, e);
                return Err(Neo4jClientError::Neo4jError(e));
            }
        }

        // Verify the relationship
        let verify_query_str = format!(
            "MATCH (a {{id: '{}'}})-[r:{}]->(b {{id: '{}'}}) RETURN r",
            start_id, rel_type, end_id
        );
        let verify_q = query(&verify_query_str);
        debug!("Verify Query_string: {}", verify_query_str);

        let mut result = self.graph.execute(verify_q).await?;
        match result.next().await {
            Ok(Some(_)) => {
                debug!("Verified relationship between {} and {}", start_id, end_id);
            },
            Ok(None) => {
                error!("Failed to verify relationship between {} and {}", start_id, end_id);
            },
            Err(e) => {
                error!("Error during verification of relationship between {} and {}: {}", start_id, end_id, e);
            }
        }

        Ok(())
    }

    pub async fn query_schema(&self) -> Result<String, Neo4jClientError> {
        let mut schema = String::new();

        // Query nodes
        let node_query_str = "CALL db.schema.nodeTypeProperties()";
        let mut node_result = self.graph.execute(query(node_query_str)).await?;
        while let Ok(Some(row)) = node_result.next().await {
            schema.push_str("Node:\n");
            match row.to::<serde_json::Value>() {
                Ok(node_properties) => {
                    schema.push_str(&format!("{:?}\n", node_properties));
                }
                Err(e) => {
                    error!("Error deserializing node properties: {:?}", e);
                }
            }
        }

        // Query relationships
        let rel_query_str = "CALL db.schema.relTypeProperties()";
        let mut rel_result = self.graph.execute(query(rel_query_str)).await?;
        while let Ok(Some(row)) = rel_result.next().await {
            schema.push_str("Relationship:\n");
            match row.to::<serde_json::Value>() {
                Ok(rel_properties) => {
                    schema.push_str(&format!("{:?}\n", rel_properties));
                }
                Err(e) => {
                    error!("Error deserializing relationship properties: {:?}", e);
                }
            }
        }

        Ok(schema)
    }

    pub async fn get_node_id_by_content(&self, content: &str) -> Result<Option<String>, Neo4jClientError> {
        let content_str = content.to_string();
        let query_str = "MATCH (n {content: $content}) RETURN n.id".to_string();
        let q = Query::new(query_str).param("content", content_str);
        debug!("Get node ID by content Query: MATCH (n {{content: $content}}) RETURN n.id, content: {}", content);

        let mut result = self.graph.execute(q).await?;
        match result.next().await {
            Ok(Some(row)) => {
                match row.get::<String>("n.id") {
                    Ok(node_id) => Ok(Some(node_id)),
                    Err(_) => Ok(None),
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(Neo4jClientError::Neo4jError(e)),
        }
    }
}
