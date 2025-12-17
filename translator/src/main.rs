use axum::{
    extract::Query,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator as DFOperator};
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

// ============================================================================
// Models matching Tempo's FetchSpansRequest (pkg/traceql/storage.go)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttributeScope {
    None,
    Resource,
    Span,
    Intrinsic,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Intrinsic {
    #[serde(rename = "trace:rootService")]
    TraceRootService,
    #[serde(rename = "trace:rootSpan")]
    TraceRootSpan,
    #[serde(rename = "trace:duration")]
    TraceDuration,
    #[serde(rename = "trace:id")]
    TraceID,
    #[serde(rename = "trace:start")]
    TraceStartTime,
    #[serde(rename = "span:id")]
    SpanID,
    #[serde(rename = "span:start")]
    SpanStartTime,
    #[serde(rename = "duration")]
    Duration,
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "status")]
    Status,
    #[serde(rename = "kind")]
    Kind,
    #[serde(rename = "parent")]
    Parent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Operator {
    None,
    #[serde(rename = "=")]
    Equal,
    #[serde(rename = "!=")]
    NotEqual,
    #[serde(rename = "<")]
    LessThan,
    #[serde(rename = "<=")]
    LessThanEqual,
    #[serde(rename = ">")]
    GreaterThan,
    #[serde(rename = ">=")]
    GreaterThanEqual,
    #[serde(rename = "=~")]
    Regex,
    #[serde(rename = "!~")]
    NotRegex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StaticType {
    String,
    Int,
    Float,
    Boolean,
    Duration,
    Status,
    Kind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Static {
    #[serde(rename = "type")]
    pub static_type: StaticType,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attribute {
    pub scope: AttributeScope,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intrinsic: Option<Intrinsic>,
    #[serde(default)]
    pub parent: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    pub attribute: Attribute,
    pub op: Operator,
    #[serde(default)]
    pub operands: Vec<Static>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchSpansRequest {
    pub start_time_unix_nanos: u64,
    pub end_time_unix_nanos: u64,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(default)]
    pub all_conditions: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default)]
    pub second_pass_conditions: Vec<Condition>,
    #[serde(default)]
    pub second_pass_select_all: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_expression: Option<FilterExpression>,
}

// ============================================================================
// Filter Expression Tree - for Go-side evaluation of SQL WHERE clause
// ============================================================================

/// FilterExpression represents a tree structure for evaluating SQL WHERE clauses.
/// Go will interpret this tree to filter spans locally without transferring span data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FilterExpression {
    /// Logical AND of two expressions
    And {
        left: Box<FilterExpression>,
        right: Box<FilterExpression>,
    },
    /// Logical OR of two expressions
    Or {
        left: Box<FilterExpression>,
        right: Box<FilterExpression>,
    },
    /// Logical NOT of an expression
    Not {
        expression: Box<FilterExpression>,
    },
    /// Comparison of an attribute against a value
    Comparison {
        attribute: Attribute,
        op: Operator,
        value: Static,
    },
    /// Check if attribute exists (is not null)
    Exists {
        attribute: Attribute,
    },
    /// Always true - used for queries without WHERE clause
    True,
}

impl Default for FetchSpansRequest {
    fn default() -> Self {
        Self {
            start_time_unix_nanos: 0,
            end_time_unix_nanos: u64::MAX,
            conditions: Vec::new(),
            all_conditions: true,
            limit: None,
            second_pass_conditions: Vec::new(),
            second_pass_select_all: false,
            filter_expression: None,
        }
    }
}

// ============================================================================
// Column Mapping - maps SQL columns to Tempo attributes
// ============================================================================

pub struct ColumnMapping {
    intrinsic_columns: HashMap<String, Intrinsic>,
    time_columns: Vec<String>,
}

impl Default for ColumnMapping {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnMapping {
    pub fn new() -> Self {
        let mut intrinsic_columns = HashMap::new();

        // Map view column names to intrinsic types (matching spans_view schema)
        intrinsic_columns.insert("traceid".into(), Intrinsic::TraceID);
        intrinsic_columns.insert("traceidtext".into(), Intrinsic::TraceID);
        intrinsic_columns.insert("spanid".into(), Intrinsic::SpanID);
        intrinsic_columns.insert("starttimeunixnano".into(), Intrinsic::TraceStartTime);
        intrinsic_columns.insert("endtimeunixnano".into(), Intrinsic::TraceStartTime);
        intrinsic_columns.insert("durationnano".into(), Intrinsic::Duration);
        intrinsic_columns.insert("rootservicename".into(), Intrinsic::TraceRootService);
        intrinsic_columns.insert("rootspanname".into(), Intrinsic::TraceRootSpan);
        intrinsic_columns.insert("span_name".into(), Intrinsic::Name);
        intrinsic_columns.insert("span_kind".into(), Intrinsic::Kind);
        intrinsic_columns.insert("status".into(), Intrinsic::Status);
        intrinsic_columns.insert("statuscode".into(), Intrinsic::Status);
        intrinsic_columns.insert("parentspanid".into(), Intrinsic::Parent);

        let time_columns = vec![
            "starttimeunixnano".into(),
            "endtimeunixnano".into(),
            "span_start".into(),
        ];

        Self {
            intrinsic_columns,
            time_columns,
        }
    }

    pub fn get_intrinsic(&self, column: &str) -> Option<&Intrinsic> {
        self.intrinsic_columns.get(&column.to_lowercase())
    }

    pub fn is_time_column(&self, column: &str) -> bool {
        self.time_columns.contains(&column.to_lowercase())
    }

    pub fn is_start_time_column(&self, column: &str) -> bool {
        let lower = column.to_lowercase();
        lower == "starttimeunixnano" || lower == "span_start"
    }

    pub fn is_end_time_column(&self, column: &str) -> bool {
        column.to_lowercase() == "endtimeunixnano"
    }
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Error, Debug)]
pub enum ConversionError {
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Unsupported expression: {0}")]
    UnsupportedExpression(String),

    #[error("Invalid column: {0}")]
    InvalidColumn(String),
}

// ============================================================================
// SQL to FetchSpansRequest Converter
// ============================================================================

pub struct SqlToFetchRequestConverter {
    column_mapping: ColumnMapping,
}

impl Default for SqlToFetchRequestConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlToFetchRequestConverter {
    pub fn new() -> Self {
        Self {
            column_mapping: ColumnMapping::new(),
        }
    }

    pub fn with_column_mapping(column_mapping: ColumnMapping) -> Self {
        Self { column_mapping }
    }

    /// Convert SQL query to FetchSpansRequest
    pub async fn convert(&self, sql: &str) -> Result<FetchSpansRequest, ConversionError> {
        let ctx = self.create_context()?;

        // Parse SQL to logical plan
        let plan = ctx.state().create_logical_plan(sql).await?;

        // Extract components from the plan
        let mut request = FetchSpansRequest::default();
        self.extract_from_plan(&plan, &mut request)?;

        // Extract filter expression tree for Go-side evaluation
        request.filter_expression = self.extract_filter_expression(&plan)?;

        Ok(request)
    }

    /// Extract filter expression tree from the logical plan
    fn extract_filter_expression(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Option<FilterExpression>, ConversionError> {
        match plan {
            LogicalPlan::Filter(filter) => {
                // Convert the filter predicate to a FilterExpression tree
                let expr = self.expr_to_filter_expression(&filter.predicate)?;
                Ok(Some(expr))
            }
            LogicalPlan::Projection(proj) => self.extract_filter_expression(&proj.input),
            LogicalPlan::Limit(limit) => self.extract_filter_expression(&limit.input),
            LogicalPlan::Sort(sort) => self.extract_filter_expression(&sort.input),
            LogicalPlan::TableScan(_) => Ok(Some(FilterExpression::True)),
            _ => {
                // Recursively search in inputs
                for input in plan.inputs() {
                    if let Some(expr) = self.extract_filter_expression(input)? {
                        if !matches!(expr, FilterExpression::True) {
                            return Ok(Some(expr));
                        }
                    }
                }
                Ok(Some(FilterExpression::True))
            }
        }
    }

    /// Convert a DataFusion expression to a FilterExpression tree
    fn expr_to_filter_expression(&self, expr: &Expr) -> Result<FilterExpression, ConversionError> {
        match expr {
            // Handle AND
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == DFOperator::And => {
                let left_expr = self.expr_to_filter_expression(left)?;
                let right_expr = self.expr_to_filter_expression(right)?;
                Ok(FilterExpression::And {
                    left: Box::new(left_expr),
                    right: Box::new(right_expr),
                })
            }

            // Handle OR
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == DFOperator::Or => {
                let left_expr = self.expr_to_filter_expression(left)?;
                let right_expr = self.expr_to_filter_expression(right)?;
                Ok(FilterExpression::Or {
                    left: Box::new(left_expr),
                    right: Box::new(right_expr),
                })
            }

            // Handle comparison operators
            Expr::BinaryExpr(binary) => {
                if let Some((attribute, op, value)) = self.binary_to_comparison(binary)? {
                    Ok(FilterExpression::Comparison {
                        attribute,
                        op,
                        value,
                    })
                } else {
                    // If we can't convert, treat as always true (won't filter anything)
                    Ok(FilterExpression::True)
                }
            }

            // Handle NOT
            Expr::Not(inner) => {
                let inner_expr = self.expr_to_filter_expression(inner)?;
                Ok(FilterExpression::Not {
                    expression: Box::new(inner_expr),
                })
            }

            // Handle LIKE -> regex comparison
            Expr::Like(like) => {
                if let Some(attribute) = self.expr_to_attribute(&like.expr)? {
                    let pattern = match &*like.pattern {
                        Expr::Literal(ScalarValue::Utf8(Some(s))) => self.like_to_regex(s),
                        _ => return Ok(FilterExpression::True),
                    };

                    let op = if like.negated {
                        Operator::NotRegex
                    } else {
                        Operator::Regex
                    };

                    Ok(FilterExpression::Comparison {
                        attribute,
                        op,
                        value: Static {
                            static_type: StaticType::String,
                            value: serde_json::Value::String(pattern),
                        },
                    })
                } else {
                    Ok(FilterExpression::True)
                }
            }

            // Handle IN lists - convert to OR of equals
            Expr::InList(in_list) => {
                if let Some(attribute) = self.expr_to_attribute(&in_list.expr)? {
                    let mut comparisons: Vec<FilterExpression> = Vec::new();

                    for item in &in_list.list {
                        if let Some(value) = self.expr_to_static(item) {
                            let op = if in_list.negated {
                                Operator::NotEqual
                            } else {
                                Operator::Equal
                            };
                            comparisons.push(FilterExpression::Comparison {
                                attribute: attribute.clone(),
                                op,
                                value,
                            });
                        }
                    }

                    if comparisons.is_empty() {
                        return Ok(FilterExpression::True);
                    }

                    // For IN: OR together all comparisons
                    // For NOT IN: AND together all not-equal comparisons
                    let combine = if in_list.negated {
                        |left, right| FilterExpression::And {
                            left: Box::new(left),
                            right: Box::new(right),
                        }
                    } else {
                        |left, right| FilterExpression::Or {
                            left: Box::new(left),
                            right: Box::new(right),
                        }
                    };

                    let result = comparisons.into_iter().reduce(combine);
                    Ok(result.unwrap_or(FilterExpression::True))
                } else {
                    Ok(FilterExpression::True)
                }
            }

            // Handle BETWEEN
            Expr::Between(between) => {
                if let Some(attribute) = self.expr_to_attribute(&between.expr)? {
                    let low = self.expr_to_static(&between.low);
                    let high = self.expr_to_static(&between.high);

                    match (low, high) {
                        (Some(low_val), Some(high_val)) => {
                            if between.negated {
                                // NOT BETWEEN: value < low OR value > high
                                Ok(FilterExpression::Or {
                                    left: Box::new(FilterExpression::Comparison {
                                        attribute: attribute.clone(),
                                        op: Operator::LessThan,
                                        value: low_val,
                                    }),
                                    right: Box::new(FilterExpression::Comparison {
                                        attribute,
                                        op: Operator::GreaterThan,
                                        value: high_val,
                                    }),
                                })
                            } else {
                                // BETWEEN: value >= low AND value <= high
                                Ok(FilterExpression::And {
                                    left: Box::new(FilterExpression::Comparison {
                                        attribute: attribute.clone(),
                                        op: Operator::GreaterThanEqual,
                                        value: low_val,
                                    }),
                                    right: Box::new(FilterExpression::Comparison {
                                        attribute,
                                        op: Operator::LessThanEqual,
                                        value: high_val,
                                    }),
                                })
                            }
                        }
                        _ => Ok(FilterExpression::True),
                    }
                } else {
                    Ok(FilterExpression::True)
                }
            }

            // Handle IS NULL
            Expr::IsNull(inner) => {
                if let Some(attribute) = self.expr_to_attribute(inner.as_ref())? {
                    // IS NULL means attribute doesn't exist - negate Exists
                    Ok(FilterExpression::Not {
                        expression: Box::new(FilterExpression::Exists { attribute }),
                    })
                } else {
                    Ok(FilterExpression::True)
                }
            }

            // Handle IS NOT NULL
            Expr::IsNotNull(inner) => {
                if let Some(attribute) = self.expr_to_attribute(inner.as_ref())? {
                    Ok(FilterExpression::Exists { attribute })
                } else {
                    Ok(FilterExpression::True)
                }
            }

            _ => {
                // For unsupported expressions, return True (won't filter)
                Ok(FilterExpression::True)
            }
        }
    }

    /// Convert a binary expression to attribute, operator, and value
    fn binary_to_comparison(
        &self,
        binary: &BinaryExpr,
    ) -> Result<Option<(Attribute, Operator, Static)>, ConversionError> {
        // Handle both column/map_access op literal and literal op column/map_access
        let (attr_expr, value_expr, reversed) =
            if self.is_attribute_expr(&binary.left) && !self.is_attribute_expr(&binary.right) {
                (&*binary.left, &*binary.right, false)
            } else if self.is_attribute_expr(&binary.right) && !self.is_attribute_expr(&binary.left)
            {
                (&*binary.right, &*binary.left, true)
            } else {
                return Ok(None);
            };

        let attribute = match self.expr_to_attribute(attr_expr)? {
            Some(attr) => attr,
            None => return Ok(None),
        };

        let value = match self.expr_to_static(value_expr) {
            Some(s) => s,
            None => return Ok(None),
        };

        let op = self.map_operator(&binary.op, reversed)?;

        Ok(Some((attribute, op, value)))
    }

    /// Create a DataFusion context with spans_view schema registered
    fn create_context(&self) -> Result<SessionContext, ConversionError> {
        let ctx = SessionContext::new();

        // Define the spans_view schema matching the Python query_transformer output
        let schema = Schema::new(vec![
            Field::new("traceid", DataType::Binary, false),
            Field::new("traceidtext", DataType::Utf8, true),
            Field::new(
                "starttimeunixnano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "endtimeunixnano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("durationnano", DataType::Int64, false),
            Field::new("rootservicename", DataType::Utf8, true),
            Field::new("rootspanname", DataType::Utf8, true),
            Field::new("spanid", DataType::Binary, false),
            Field::new("span_name", DataType::Utf8, true),
            Field::new("span_kind", DataType::Int32, true),
            Field::new("span_start", DataType::Int64, true),
            Field::new("span_duration", DataType::Int64, true),
            Field::new("statuscode", DataType::Int32, true),
            Field::new("parentspanid", DataType::Binary, true),
            // Common span attributes as pivoted columns
            Field::new("http_method", DataType::Utf8, true),
            Field::new("http_url", DataType::Utf8, true),
            Field::new("http_status_code", DataType::Int64, true),
            Field::new("service_name", DataType::Utf8, true),
            // Map field for dynamic span attributes: span_attributes['key']
            Field::new(
                "span_attributes",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ])),
                        false,
                    )),
                    false, // sorted
                ),
                true, // nullable
            ),
            // Map field for dynamic resource attributes: resource_attributes['key']
            Field::new(
                "resource_attributes",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ])),
                        false,
                    )),
                    false, // sorted
                ),
                true, // nullable
            ),
        ]);

        // Register empty table with schema (we only need schema for planning)
        let schema_ref = Arc::new(schema);
        let _df_schema = DFSchema::try_from(schema_ref.clone())?;
        ctx.register_table(
            "spans_view",
            Arc::new(datafusion::datasource::empty::EmptyTable::new(schema_ref)),
        )?;

        Ok(ctx)
    }

    /// Recursively extract components from the logical plan
    fn extract_from_plan(
        &self,
        plan: &LogicalPlan,
        request: &mut FetchSpansRequest,
    ) -> Result<(), ConversionError> {
        match plan {
            LogicalPlan::Projection(proj) => {
                // Extract SELECT columns for second_pass_conditions
                self.extract_projections(&proj.expr, request)?;
                self.extract_from_plan(&proj.input, request)?;
            }
            LogicalPlan::Filter(filter) => {
                // Extract WHERE conditions
                self.extract_filter_conditions(&filter.predicate, request)?;
                self.extract_from_plan(&filter.input, request)?;
            }
            LogicalPlan::Limit(limit) => {
                // Extract LIMIT
                if let Some(ref fetch) = limit.fetch {
                    // fetch is an Expr, need to evaluate it
                    if let Expr::Literal(ScalarValue::Int64(Some(n))) = fetch.as_ref() {
                        request.limit = Some(*n as usize);
                    } else if let Expr::Literal(ScalarValue::UInt64(Some(n))) = fetch.as_ref() {
                        request.limit = Some(*n as usize);
                    }
                }
                self.extract_from_plan(&limit.input, request)?;
            }
            LogicalPlan::Sort(sort) => {
                // Skip sort, continue to input
                self.extract_from_plan(&sort.input, request)?;
            }
            LogicalPlan::TableScan(_) => {
                // Base case - we've reached the table
            }
            _ => {
                // Recursively process other plan nodes
                for input in plan.inputs() {
                    self.extract_from_plan(input, request)?;
                }
            }
        }
        Ok(())
    }

    /// Extract SELECT columns as second pass conditions (OpNone - select without filter)
    fn extract_projections(
        &self,
        exprs: &[Expr],
        request: &mut FetchSpansRequest,
    ) -> Result<(), ConversionError> {
        for expr in exprs {
            // Handle both direct columns and map access (span_attributes['key'])
            if let Some(attribute) = self.expr_to_attribute(expr)? {
                // OpNone means "select this attribute" without filtering
                request.second_pass_conditions.push(Condition {
                    attribute,
                    op: Operator::None,
                    operands: Vec::new(),
                });
            }
        }
        Ok(())
    }

    /// Extract conditions from a filter expression
    fn extract_filter_conditions(
        &self,
        expr: &Expr,
        request: &mut FetchSpansRequest,
    ) -> Result<(), ConversionError> {
        match expr {
            // Handle AND - all conditions must match
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == DFOperator::And => {
                // Don't set all_conditions = true here, as a parent OR should override
                self.extract_filter_conditions(left, request)?;
                self.extract_filter_conditions(right, request)?;
            }

            // Handle OR - any condition matches
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == DFOperator::Or => {
                // Once we see any OR, the storage layer must be permissive.
                // The actual filtering logic is handled by filter_expression.
                request.all_conditions = false;
                self.extract_filter_conditions(left, request)?;
                self.extract_filter_conditions(right, request)?;
            }

            // Handle comparison operators (=, !=, <, <=, >, >=)
            Expr::BinaryExpr(binary) => {
                if let Some(condition) = self.binary_expr_to_condition(binary)? {
                    // Check for time range conditions and update accordingly
                    if let Some(col_name) = self.get_column_name(&binary.left) {
                        if self.column_mapping.is_time_column(&col_name) {
                            self.update_time_range(&condition, &col_name, request);
                        } else {
                            request.conditions.push(condition);
                        }
                    } else {
                        request.conditions.push(condition);
                    }
                }
            }

            // Handle LIKE -> regex conditions
            Expr::Like(like) => {
                if let Some(condition) = self.like_to_condition(like)? {
                    request.conditions.push(condition);
                }
            }

            // Handle NOT LIKE
            Expr::SimilarTo(similar) => {
                if let Some(condition) = self.similar_to_condition(similar)? {
                    request.conditions.push(condition);
                }
            }

            // Handle IN lists
            Expr::InList(in_list) => {
                let conditions = self.in_list_to_conditions(in_list)?;
                // For IN lists, we want OR semantics for those specific conditions
                request.conditions.extend(conditions);
            }

            // Handle BETWEEN
            Expr::Between(between) => {
                let conditions = self.between_to_conditions(between)?;
                for condition in conditions {
                    if let Some(col_name) = self.get_column_name(&between.expr) {
                        if self.column_mapping.is_time_column(&col_name) {
                            self.update_time_range(&condition, &col_name, request);
                        } else {
                            request.conditions.push(condition);
                        }
                    } else {
                        request.conditions.push(condition);
                    }
                }
            }

            // Handle IS NULL
            Expr::IsNull(inner) => {
                if let Some(attribute) = self.expr_to_attribute(inner.as_ref())? {
                    // IS NULL is typically used to check if attribute doesn't exist
                    // In TraceQL, this would be checking for absence
                    request.conditions.push(Condition {
                        attribute,
                        op: Operator::Equal,
                        operands: vec![Static {
                            static_type: StaticType::String,
                            value: serde_json::Value::Null,
                        }],
                    });
                }
            }

            // Handle IS NOT NULL
            Expr::IsNotNull(inner) => {
                if let Some(attribute) = self.expr_to_attribute(inner.as_ref())? {
                    // IS NOT NULL checks for attribute existence (OpNone in TraceQL)
                    request.conditions.push(Condition {
                        attribute,
                        op: Operator::None,
                        operands: Vec::new(),
                    });
                }
            }

            // Handle NOT expressions
            Expr::Not(inner) => {
                // For NOT(condition), we'd need to negate the operator
                // This is a simplification - full NOT support would need more work
                self.extract_filter_conditions(inner, request)?;
            }

            _ => {
                // Log unsupported expression types but don't fail
                eprintln!("Warning: Unsupported filter expression: {:?}", expr);
            }
        }
        Ok(())
    }

    /// Get column name from an expression if it's a column reference
    fn get_column_name(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Column(col) => Some(col.name.clone()),
            // Handle map access: span_attributes['key'] -> returns "span_attributes"
            Expr::ScalarFunction(func) => {
                if func.name() == "get_field" {
                    if let Some(Expr::Column(col)) = func.args.first() {
                        return Some(col.name.clone());
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Extract attribute from a map access expression like span_attributes['key']
    /// Returns the attribute key (e.g., "key" from span_attributes['key'])
    fn extract_map_access_key(&self, expr: &Expr) -> Option<String> {
        if let Expr::ScalarFunction(func) = expr {
            if func.name() == "get_field" && func.args.len() == 2 {
                // First arg is the map column (span_attributes)
                // Second arg is the key literal
                if let Expr::Literal(ScalarValue::Utf8(Some(key))) = &func.args[1] {
                    return Some(key.clone());
                }
            }
        }
        None
    }

    /// Extract the map column name from a get_field expression
    fn extract_map_column_name(&self, expr: &Expr) -> Option<String> {
        if let Expr::ScalarFunction(func) = expr {
            if func.name() == "get_field" {
                if let Some(Expr::Column(col)) = func.args.first() {
                    return Some(col.name.clone());
                }
            }
        }
        None
    }

    /// Convert an expression to an Attribute - handles both Column and map access
    fn expr_to_attribute(&self, expr: &Expr) -> Result<Option<Attribute>, ConversionError> {
        match expr {
            Expr::Column(col) => Ok(Some(self.column_to_attribute(&col.name)?)),
            // Handle map access: span_attributes['key'] or resource_attributes['key']
            Expr::ScalarFunction(func) if func.name() == "get_field" => {
                let map_col = self.extract_map_column_name(expr);
                let key = self.extract_map_access_key(expr);

                match (map_col.as_deref(), key) {
                    (Some("span_attributes"), Some(key)) => Ok(Some(Attribute {
                        scope: AttributeScope::Span,
                        name: key,
                        intrinsic: None,
                        parent: false,
                    })),
                    (Some("resource_attributes"), Some(key)) => Ok(Some(Attribute {
                        scope: AttributeScope::Resource,
                        name: key,
                        intrinsic: None,
                        parent: false,
                    })),
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    /// Update time range based on condition
    fn update_time_range(
        &self,
        condition: &Condition,
        col_name: &str,
        request: &mut FetchSpansRequest,
    ) {
        if condition.operands.is_empty() {
            return;
        }

        let value = match &condition.operands[0].value {
            serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => return,
        };

        let is_start = self.column_mapping.is_start_time_column(col_name);

        match condition.op {
            Operator::GreaterThan | Operator::GreaterThanEqual => {
                if is_start {
                    request.start_time_unix_nanos = request.start_time_unix_nanos.max(value);
                }
            }
            Operator::LessThan | Operator::LessThanEqual => {
                if is_start || self.column_mapping.is_end_time_column(col_name) {
                    request.end_time_unix_nanos = request.end_time_unix_nanos.min(value);
                }
            }
            Operator::Equal => {
                if is_start {
                    request.start_time_unix_nanos = value;
                    request.end_time_unix_nanos = value;
                }
            }
            _ => {}
        }
    }

    /// Check if expression is a column or map access (can be used as an attribute)
    fn is_attribute_expr(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::Column(_))
            || matches!(expr, Expr::ScalarFunction(f) if f.name() == "get_field")
    }

    /// Convert a binary expression to a Condition
    fn binary_expr_to_condition(
        &self,
        binary: &BinaryExpr,
    ) -> Result<Option<Condition>, ConversionError> {
        // Handle both column/map_access op literal and literal op column/map_access
        let (attr_expr, value_expr, reversed) =
            if self.is_attribute_expr(&binary.left) && !self.is_attribute_expr(&binary.right) {
                (&*binary.left, &*binary.right, false)
            } else if self.is_attribute_expr(&binary.right) && !self.is_attribute_expr(&binary.left)
            {
                (&*binary.right, &*binary.left, true)
            } else {
                return Ok(None);
            };

        // Extract attribute from column or map access
        let attribute = match self.expr_to_attribute(attr_expr)? {
            Some(attr) => attr,
            None => return Ok(None),
        };

        // Extract literal value
        let operand = match self.expr_to_static(value_expr) {
            Some(s) => s,
            None => return Ok(None),
        };

        // Map DataFusion operator to Tempo operator (reverse if needed)
        let op = self.map_operator(&binary.op, reversed)?;

        Ok(Some(Condition {
            attribute,
            op,
            operands: vec![operand],
        }))
    }

    /// Map DataFusion operator to Tempo operator
    fn map_operator(
        &self,
        df_op: &DFOperator,
        reversed: bool,
    ) -> Result<Operator, ConversionError> {
        let op = match df_op {
            DFOperator::Eq => Operator::Equal,
            DFOperator::NotEq => Operator::NotEqual,
            DFOperator::Lt => {
                if reversed {
                    Operator::GreaterThan
                } else {
                    Operator::LessThan
                }
            }
            DFOperator::LtEq => {
                if reversed {
                    Operator::GreaterThanEqual
                } else {
                    Operator::LessThanEqual
                }
            }
            DFOperator::Gt => {
                if reversed {
                    Operator::LessThan
                } else {
                    Operator::GreaterThan
                }
            }
            DFOperator::GtEq => {
                if reversed {
                    Operator::LessThanEqual
                } else {
                    Operator::GreaterThanEqual
                }
            }
            DFOperator::RegexMatch => Operator::Regex,
            DFOperator::RegexNotMatch => Operator::NotRegex,
            _ => {
                return Err(ConversionError::UnsupportedExpression(format!(
                    "Operator {:?}",
                    df_op
                )))
            }
        };
        Ok(op)
    }

    /// Convert LIKE expression to regex Condition
    fn like_to_condition(
        &self,
        like: &datafusion::logical_expr::Like,
    ) -> Result<Option<Condition>, ConversionError> {
        let attribute = match self.expr_to_attribute(&like.expr)? {
            Some(attr) => attr,
            None => return Ok(None),
        };

        let pattern = match &*like.pattern {
            Expr::Literal(ScalarValue::Utf8(Some(s))) => {
                // Convert SQL LIKE pattern to regex
                self.like_to_regex(s)
            }
            _ => return Ok(None),
        };

        Ok(Some(Condition {
            attribute,
            op: if like.negated {
                Operator::NotRegex
            } else {
                Operator::Regex
            },
            operands: vec![Static {
                static_type: StaticType::String,
                value: serde_json::Value::String(pattern),
            }],
        }))
    }

    /// Convert SIMILAR TO expression to regex Condition
    fn similar_to_condition(
        &self,
        similar: &datafusion::logical_expr::Like,
    ) -> Result<Option<Condition>, ConversionError> {
        // SIMILAR TO is already regex-like in SQL
        let attribute = match self.expr_to_attribute(&similar.expr)? {
            Some(attr) => attr,
            None => return Ok(None),
        };

        let pattern = match &*similar.pattern {
            Expr::Literal(ScalarValue::Utf8(Some(s))) => s.clone(),
            _ => return Ok(None),
        };

        Ok(Some(Condition {
            attribute,
            op: if similar.negated {
                Operator::NotRegex
            } else {
                Operator::Regex
            },
            operands: vec![Static {
                static_type: StaticType::String,
                value: serde_json::Value::String(pattern),
            }],
        }))
    }

    /// Convert IN list to multiple conditions
    fn in_list_to_conditions(&self, in_list: &InList) -> Result<Vec<Condition>, ConversionError> {
        let attribute = match self.expr_to_attribute(&in_list.expr)? {
            Some(attr) => attr,
            None => return Ok(Vec::new()),
        };

        let mut conditions = Vec::new();
        for item in &in_list.list {
            if let Some(operand) = self.expr_to_static(item) {
                conditions.push(Condition {
                    attribute: attribute.clone(),
                    op: if in_list.negated {
                        Operator::NotEqual
                    } else {
                        Operator::Equal
                    },
                    operands: vec![operand],
                });
            }
        }

        Ok(conditions)
    }

    /// Convert BETWEEN to two conditions
    fn between_to_conditions(
        &self,
        between: &datafusion::logical_expr::Between,
    ) -> Result<Vec<Condition>, ConversionError> {
        let attribute = match &*between.expr {
            Expr::Column(col) => self.column_to_attribute(&col.name)?,
            _ => return Ok(Vec::new()),
        };

        let mut conditions = Vec::new();

        if let Some(low) = self.expr_to_static(&between.low) {
            conditions.push(Condition {
                attribute: attribute.clone(),
                op: if between.negated {
                    Operator::LessThan
                } else {
                    Operator::GreaterThanEqual
                },
                operands: vec![low],
            });
        }

        if let Some(high) = self.expr_to_static(&between.high) {
            conditions.push(Condition {
                attribute: attribute.clone(),
                op: if between.negated {
                    Operator::GreaterThan
                } else {
                    Operator::LessThanEqual
                },
                operands: vec![high],
            });
        }

        Ok(conditions)
    }

    /// Convert SQL LIKE pattern to regex pattern
    fn like_to_regex(&self, pattern: &str) -> String {
        let mut regex = String::from("^");
        let mut chars = pattern.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                '%' => regex.push_str(".*"),
                '_' => regex.push('.'),
                '\\' => {
                    // Escape sequence
                    if let Some(&next) = chars.peek() {
                        if next == '%' || next == '_' {
                            regex.push(chars.next().unwrap());
                        } else {
                            regex.push_str("\\\\");
                        }
                    }
                }
                // Escape regex special characters
                '.' | '*' | '+' | '?' | '^' | '$' | '{' | '}' | '[' | ']' | '|' | '(' | ')' => {
                    regex.push('\\');
                    regex.push(c);
                }
                _ => regex.push(c),
            }
        }

        regex.push('$');
        regex
    }

    /// Convert expression to Static value
    fn expr_to_static(&self, expr: &Expr) -> Option<Static> {
        match expr {
            Expr::Literal(scalar) => self.scalar_to_static(scalar),
            _ => None,
        }
    }

    /// Convert ScalarValue to Static
    fn scalar_to_static(&self, scalar: &ScalarValue) -> Option<Static> {
        match scalar {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(Static {
                static_type: StaticType::String,
                value: serde_json::Value::String(s.clone()),
            }),
            ScalarValue::Int8(Some(n)) => Some(Static {
                static_type: StaticType::Int,
                value: serde_json::Value::Number((*n as i64).into()),
            }),
            ScalarValue::Int16(Some(n)) => Some(Static {
                static_type: StaticType::Int,
                value: serde_json::Value::Number((*n as i64).into()),
            }),
            ScalarValue::Int32(Some(n)) => Some(Static {
                static_type: StaticType::Int,
                value: serde_json::Value::Number((*n as i64).into()),
            }),
            ScalarValue::Int64(Some(n)) => Some(Static {
                static_type: StaticType::Int,
                value: serde_json::Value::Number((*n).into()),
            }),
            ScalarValue::UInt8(Some(n)) => Some(Static {
                static_type: StaticType::Int,
                value: serde_json::Value::Number((*n as u64).into()),
            }),
            ScalarValue::UInt16(Some(n)) => Some(Static {
                static_type: StaticType::Int,
                value: serde_json::Value::Number((*n as u64).into()),
            }),
            ScalarValue::UInt32(Some(n)) => Some(Static {
                static_type: StaticType::Int,
                value: serde_json::Value::Number((*n as u64).into()),
            }),
            ScalarValue::UInt64(Some(n)) => Some(Static {
                static_type: StaticType::Int,
                value: serde_json::Value::Number((*n).into()),
            }),
            ScalarValue::Float32(Some(n)) => {
                serde_json::Number::from_f64(*n as f64).map(|num| Static {
                    static_type: StaticType::Float,
                    value: serde_json::Value::Number(num),
                })
            }
            ScalarValue::Float64(Some(n)) => serde_json::Number::from_f64(*n).map(|num| Static {
                static_type: StaticType::Float,
                value: serde_json::Value::Number(num),
            }),
            ScalarValue::Boolean(Some(b)) => Some(Static {
                static_type: StaticType::Boolean,
                value: serde_json::Value::Bool(*b),
            }),
            ScalarValue::Null => Some(Static {
                static_type: StaticType::String,
                value: serde_json::Value::Null,
            }),
            _ => None,
        }
    }

    /// Map SQL column name to Tempo Attribute
    fn column_to_attribute(&self, column_name: &str) -> Result<Attribute, ConversionError> {
        // Check intrinsic mappings first
        if let Some(intrinsic) = self.column_mapping.get_intrinsic(column_name) {
            return Ok(Attribute {
                scope: AttributeScope::Intrinsic,
                name: column_name.to_string(),
                intrinsic: Some(intrinsic.clone()),
                parent: false,
            });
        }

        // Check for resource attributes (resource_*)
        if let Some(attr_name) = column_name.strip_prefix("resource_") {
            return Ok(Attribute {
                scope: AttributeScope::Resource,
                name: attr_name.replace('_', "."),
                intrinsic: None,
                parent: false,
            });
        }

        // Default to span attribute (convert underscores back to dots)
        Ok(Attribute {
            scope: AttributeScope::Span,
            name: column_name.replace('_', "."),
            intrinsic: None,
            parent: false,
        })
    }
}

// ============================================================================
// HTTP Server
// ============================================================================

#[derive(Debug, Deserialize)]
struct SqlQuery {
    q: String,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

async fn convert_sql_handler(
    Query(params): Query<SqlQuery>,
) -> Result<Json<FetchSpansRequest>, (StatusCode, Json<ErrorResponse>)> {
    let converter = SqlToFetchRequestConverter::new();

    match converter.convert(&params.q).await {
        Ok(request) => Ok(Json(request)),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

async fn health_handler() -> impl IntoResponse {
    "OK"
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    // Check for CLI mode vs server mode
    if args.len() > 1 && args[1] != "--server" {
        // CLI mode: convert SQL from command line argument
        let sql = args[1].clone();
        let converter = SqlToFetchRequestConverter::new();
        let request = converter.convert(&sql).await?;
        let json = serde_json::to_string_pretty(&request)?;
        println!("{}", json);
        return Ok(());
    }

    // Server mode
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    let addr = format!("0.0.0.0:{}", port);

    let app = Router::new()
        .route("/convert", get(convert_sql_handler))
        .route("/health", get(health_handler));

    println!("Starting server on {}", addr);
    println!("Usage: GET /convert?q=<SQL_QUERY>");

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_simple_query() {
        let sql = r#"
            SELECT traceid, spanid
            FROM spans_view
            WHERE rootservicename = 'my-service'
            LIMIT 10
        "#;

        let converter = SqlToFetchRequestConverter::new();
        let request = converter.convert(sql).await.unwrap();

        assert_eq!(request.limit, Some(10));
        assert_eq!(request.conditions.len(), 1);
        assert_eq!(request.conditions[0].op, Operator::Equal);
    }

    #[tokio::test]
    async fn test_span_attributes_query() {
        let sql = r#"
            SELECT span_attributes['attr']
            FROM spans_view
            WHERE span_attributes['attr'] != 'val1'
            LIMIT 10
        "#;

        let converter = SqlToFetchRequestConverter::new();
        let request = converter.convert(sql).await.unwrap();

        assert_eq!(request.limit, Some(10));
        assert_eq!(request.conditions.len(), 1);
        assert_eq!(request.conditions[0].op, Operator::NotEqual);
    }

    #[tokio::test]
    async fn test_time_range_extraction() {
        let sql = r#"
            SELECT traceid
            FROM spans_view
            WHERE starttimeunixnano >= 1000000000
              AND starttimeunixnano <= 2000000000
        "#;

        let converter = SqlToFetchRequestConverter::new();
        let request = converter.convert(sql).await.unwrap();

        assert_eq!(request.start_time_unix_nanos, 1000000000);
        assert_eq!(request.end_time_unix_nanos, 2000000000);
        // Time conditions should NOT be in the conditions list
        assert!(request.conditions.is_empty());
    }

    #[tokio::test]
    async fn test_multiple_conditions() {
        let sql = r#"
            SELECT traceid
            FROM spans_view
            WHERE rootservicename = 'svc1'
              AND http_method = 'POST'
              AND http_status_code = 500
        "#;

        let converter = SqlToFetchRequestConverter::new();
        let request = converter.convert(sql).await.unwrap();

        assert!(request.all_conditions);
        assert_eq!(request.conditions.len(), 3);
    }

    #[tokio::test]
    async fn test_like_to_regex() {
        let converter = SqlToFetchRequestConverter::new();

        assert_eq!(converter.like_to_regex("hello%"), "^hello.*$");
        assert_eq!(converter.like_to_regex("%world"), "^.*world$");
        assert_eq!(converter.like_to_regex("foo_bar"), "^foo.bar$");
        assert_eq!(converter.like_to_regex("%test%"), "^.*test.*$");
    }

    #[tokio::test]
    async fn test_attribute_scoping() {
        let converter = SqlToFetchRequestConverter::new();

        // Intrinsic
        let attr = converter.column_to_attribute("traceid").unwrap();
        assert_eq!(attr.scope, AttributeScope::Intrinsic);
        assert!(attr.intrinsic.is_some());

        // Resource
        let attr = converter
            .column_to_attribute("resource_service_name")
            .unwrap();
        assert_eq!(attr.scope, AttributeScope::Resource);
        assert_eq!(attr.name, "service.name");

        // Span (default)
        let attr = converter.column_to_attribute("http_method").unwrap();
        assert_eq!(attr.scope, AttributeScope::Span);
        assert_eq!(attr.name, "http.method");
    }

    #[tokio::test]
    async fn test_filter_expression_and() {
        let sql = r#"
            SELECT traceid
            FROM spans_view
            WHERE rootservicename = 'svc1' AND http_status_code = 500
        "#;

        let converter = SqlToFetchRequestConverter::new();
        let request = converter.convert(sql).await.unwrap();

        // Should have a filter expression
        assert!(request.filter_expression.is_some());
        let expr = request.filter_expression.unwrap();

        // Should be an AND expression
        match expr {
            FilterExpression::And { left, right } => {
                // Left should be a comparison
                match *left {
                    FilterExpression::Comparison { attribute, op, .. } => {
                        assert_eq!(attribute.intrinsic, Some(Intrinsic::TraceRootService));
                        assert_eq!(op, Operator::Equal);
                    }
                    _ => panic!("Expected Comparison on left side of AND"),
                }
                // Right should be a comparison
                match *right {
                    FilterExpression::Comparison { attribute, op, .. } => {
                        assert_eq!(attribute.name, "http.status.code");
                        assert_eq!(op, Operator::Equal);
                    }
                    _ => panic!("Expected Comparison on right side of AND"),
                }
            }
            _ => panic!("Expected AND expression, got {:?}", expr),
        }
    }

    #[tokio::test]
    async fn test_filter_expression_or() {
        let sql = r#"
            SELECT traceid
            FROM spans_view
            WHERE http_method = 'GET' OR http_method = 'POST'
        "#;

        let converter = SqlToFetchRequestConverter::new();
        let request = converter.convert(sql).await.unwrap();

        assert!(request.filter_expression.is_some());
        let expr = request.filter_expression.unwrap();

        // Should be an OR expression
        match expr {
            FilterExpression::Or { .. } => {}
            _ => panic!("Expected OR expression, got {:?}", expr),
        }
    }

    #[tokio::test]
    async fn test_filter_expression_serialization() {
        let sql = r#"
            SELECT traceid
            FROM spans_view
            WHERE rootservicename = 'my-service'
        "#;

        let converter = SqlToFetchRequestConverter::new();
        let request = converter.convert(sql).await.unwrap();

        // Serialize to JSON and check the structure
        let json = serde_json::to_string_pretty(&request).unwrap();
        
        // Should contain filter_expression
        assert!(json.contains("filter_expression"));
        assert!(json.contains("comparison"));
    }

    #[tokio::test]
    async fn test_filter_expression_no_where() {
        let sql = r#"
            SELECT traceid
            FROM spans_view
            LIMIT 10
        "#;

        let converter = SqlToFetchRequestConverter::new();
        let request = converter.convert(sql).await.unwrap();

        // Should have a filter expression that is True (no filtering)
        assert!(request.filter_expression.is_some());
        match request.filter_expression.unwrap() {
            FilterExpression::True => {}
            other => panic!("Expected True expression for no WHERE clause, got {:?}", other),
        }
    }
}
