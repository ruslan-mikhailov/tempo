use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator as DFOperator};
use datafusion::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

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
// Intrinsic mappings
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Intrinsic {
    TraceRootService,
    TraceRootSpan,
    TraceDuration,
    TraceID,
    TraceStartTime,
    SpanID,
    SpanStartTime,
    Duration,
    Name,
    Status,
    Kind,
    Parent,
}

impl Intrinsic {
    fn to_traceql(&self) -> &'static str {
        match self {
            Intrinsic::TraceRootService => "rootServiceName",
            Intrinsic::TraceRootSpan => "rootName",
            Intrinsic::TraceDuration => "traceDuration",
            Intrinsic::TraceID => "trace:id",
            Intrinsic::TraceStartTime => "trace:start",
            Intrinsic::SpanID => "span:id",
            Intrinsic::SpanStartTime => "span:start",
            Intrinsic::Duration => "duration",
            Intrinsic::Name => "name",
            Intrinsic::Status => "status",
            Intrinsic::Kind => "kind",
            Intrinsic::Parent => "parent",
        }
    }
}

// ============================================================================
// Column Mapping
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
}

// ============================================================================
// Attribute representation for TraceQL output
// ============================================================================

#[derive(Debug, Clone)]
pub enum AttributeScope {
    Span,
    Resource,
    Intrinsic(Intrinsic),
}

#[derive(Debug, Clone)]
pub struct TraceQLAttribute {
    pub scope: AttributeScope,
    pub name: String,
}

impl TraceQLAttribute {
    fn to_traceql(&self) -> String {
        match &self.scope {
            AttributeScope::Intrinsic(i) => i.to_traceql().to_string(),
            AttributeScope::Resource => format!("resource.{}", self.name),
            AttributeScope::Span => format!("span.{}", self.name),
        }
    }
}

// ============================================================================
// SQL to TraceQL Converter
// ============================================================================

pub struct SqlToTraceQLConverter {
    column_mapping: ColumnMapping,
}

impl Default for SqlToTraceQLConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlToTraceQLConverter {
    pub fn new() -> Self {
        Self {
            column_mapping: ColumnMapping::new(),
        }
    }

    /// Convert SQL query to TraceQL
    pub async fn convert(&self, sql: &str) -> Result<String, ConversionError> {
        let ctx = self.create_context()?;

        // Parse SQL to logical plan
        let plan = ctx.state().create_logical_plan(sql).await?;

        // Extract filter expression and convert to TraceQL
        let filter_expr = self.extract_filter_expr(&plan)?;
        let select_attrs = self.extract_select_attrs(&plan)?;

        // Build TraceQL query
        let mut traceql = String::new();

        // Add the filter (spanset selector)
        if let Some(expr) = filter_expr {
            traceql.push_str("{ ");
            traceql.push_str(&expr);
            traceql.push_str(" }");
        } else {
            traceql.push_str("{ }");
        }

        // Add select if there are non-intrinsic attributes
        let select_parts: Vec<String> = select_attrs
            .iter()
            .filter(|a| !matches!(a.scope, AttributeScope::Intrinsic(_)))
            .map(|a| a.to_traceql())
            .collect();

        if !select_parts.is_empty() {
            traceql.push_str(" | select(");
            traceql.push_str(&select_parts.join(", "));
            traceql.push(')');
        }

        Ok(traceql)
    }

    /// Create a DataFusion context with spans_view schema registered
    fn create_context(&self) -> Result<SessionContext, ConversionError> {
        let ctx = SessionContext::new();

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
            Field::new("http_method", DataType::Utf8, true),
            Field::new("http_url", DataType::Utf8, true),
            Field::new("http_status_code", DataType::Int64, true),
            Field::new("service_name", DataType::Utf8, true),
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
                    false,
                ),
                true,
            ),
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
                    false,
                ),
                true,
            ),
        ]);

        let schema_ref = Arc::new(schema);
        let _df_schema = DFSchema::try_from(schema_ref.clone())?;
        ctx.register_table(
            "spans_view",
            Arc::new(datafusion::datasource::empty::EmptyTable::new(schema_ref)),
        )?;

        Ok(ctx)
    }

    /// Extract filter expression from the logical plan
    fn extract_filter_expr(&self, plan: &LogicalPlan) -> Result<Option<String>, ConversionError> {
        match plan {
            LogicalPlan::Filter(filter) => {
                let expr = self.expr_to_traceql(&filter.predicate)?;
                Ok(Some(expr))
            }
            LogicalPlan::Projection(proj) => self.extract_filter_expr(&proj.input),
            LogicalPlan::Limit(limit) => self.extract_filter_expr(&limit.input),
            LogicalPlan::Sort(sort) => self.extract_filter_expr(&sort.input),
            LogicalPlan::TableScan(_) => Ok(None),
            _ => {
                for input in plan.inputs() {
                    if let Some(expr) = self.extract_filter_expr(input)? {
                        return Ok(Some(expr));
                    }
                }
                Ok(None)
            }
        }
    }

    /// Extract SELECT attributes from the logical plan
    fn extract_select_attrs(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Vec<TraceQLAttribute>, ConversionError> {
        match plan {
            LogicalPlan::Projection(proj) => {
                let mut attrs = Vec::new();
                for expr in &proj.expr {
                    if let Some(attr) = self.expr_to_attribute(expr)? {
                        attrs.push(attr);
                    }
                }
                Ok(attrs)
            }
            LogicalPlan::Limit(limit) => self.extract_select_attrs(&limit.input),
            LogicalPlan::Sort(sort) => self.extract_select_attrs(&sort.input),
            _ => Ok(Vec::new()),
        }
    }

    /// Convert a DataFusion expression to TraceQL string
    fn expr_to_traceql(&self, expr: &Expr) -> Result<String, ConversionError> {
        match expr {
            // Handle AND
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == DFOperator::And => {
                let left_str = self.expr_to_traceql(left)?;
                let right_str = self.expr_to_traceql(right)?;
                Ok(format!("{} && {}", left_str, right_str))
            }

            // Handle OR
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == DFOperator::Or => {
                let left_str = self.expr_to_traceql(left)?;
                let right_str = self.expr_to_traceql(right)?;
                Ok(format!("({} || {})", left_str, right_str))
            }

            // Handle comparison operators
            Expr::BinaryExpr(binary) => self.binary_to_traceql(binary),

            // Handle NOT
            Expr::Not(inner) => {
                let inner_str = self.expr_to_traceql(inner)?;
                Ok(format!("!({})", inner_str))
            }

            // Handle LIKE -> regex
            Expr::Like(like) => {
                if let Some(attr) = self.expr_to_attribute(&like.expr)? {
                    let pattern = match &*like.pattern {
                        Expr::Literal(ScalarValue::Utf8(Some(s))) => self.like_to_regex(s),
                        _ => return Err(ConversionError::UnsupportedExpression("LIKE pattern".into())),
                    };

                    let op = if like.negated { "!~" } else { "=~" };
                    Ok(format!("{} {} \"{}\"", attr.to_traceql(), op, escape_string(&pattern)))
                } else {
                    Err(ConversionError::UnsupportedExpression("LIKE expression".into()))
                }
            }

            // Handle IN lists
            Expr::InList(in_list) => self.in_list_to_traceql(in_list),

            // Handle BETWEEN
            Expr::Between(between) => self.between_to_traceql(between),

            // Handle IS NULL
            Expr::IsNull(inner) => {
                if let Some(attr) = self.expr_to_attribute(inner.as_ref())? {
                    Ok(format!("{} = nil", attr.to_traceql()))
                } else {
                    Err(ConversionError::UnsupportedExpression("IS NULL".into()))
                }
            }

            // Handle IS NOT NULL
            Expr::IsNotNull(inner) => {
                if let Some(attr) = self.expr_to_attribute(inner.as_ref())? {
                    Ok(format!("{} != nil", attr.to_traceql()))
                } else {
                    Err(ConversionError::UnsupportedExpression("IS NOT NULL".into()))
                }
            }

            _ => Err(ConversionError::UnsupportedExpression(format!("{:?}", expr))),
        }
    }

    /// Convert a binary expression to TraceQL
    fn binary_to_traceql(&self, binary: &BinaryExpr) -> Result<String, ConversionError> {
        let (attr_expr, value_expr, reversed) =
            if self.is_attribute_expr(&binary.left) && !self.is_attribute_expr(&binary.right) {
                (&*binary.left, &*binary.right, false)
            } else if self.is_attribute_expr(&binary.right) && !self.is_attribute_expr(&binary.left)
            {
                (&*binary.right, &*binary.left, true)
            } else {
                return Err(ConversionError::UnsupportedExpression(
                    "Binary expression without attribute".into(),
                ));
            };

        let attr = match self.expr_to_attribute(attr_expr)? {
            Some(a) => a,
            None => {
                return Err(ConversionError::UnsupportedExpression(
                    "Unknown attribute".into(),
                ))
            }
        };

        let value = self.expr_to_traceql_value(value_expr)?;
        let op = self.map_operator(&binary.op, reversed)?;

        Ok(format!("{} {} {}", attr.to_traceql(), op, value))
    }

    /// Convert IN list to TraceQL (OR of equals)
    fn in_list_to_traceql(&self, in_list: &InList) -> Result<String, ConversionError> {
        let attr = match self.expr_to_attribute(&in_list.expr)? {
            Some(a) => a,
            None => return Err(ConversionError::UnsupportedExpression("IN list".into())),
        };

        let attr_str = attr.to_traceql();
        let op = if in_list.negated { "!=" } else { "=" };

        let mut conditions: Vec<String> = Vec::new();
        for item in &in_list.list {
            let value = self.expr_to_traceql_value(item)?;
            conditions.push(format!("{} {} {}", attr_str, op, value));
        }

        if in_list.negated {
            // NOT IN: all must be not equal (AND)
            Ok(conditions.join(" && "))
        } else {
            // IN: any can be equal (OR)
            Ok(format!("({})", conditions.join(" || ")))
        }
    }

    /// Convert BETWEEN to TraceQL
    fn between_to_traceql(
        &self,
        between: &datafusion::logical_expr::Between,
    ) -> Result<String, ConversionError> {
        let attr = match self.expr_to_attribute(&between.expr)? {
            Some(a) => a,
            None => return Err(ConversionError::UnsupportedExpression("BETWEEN".into())),
        };

        let attr_str = attr.to_traceql();
        let low = self.expr_to_traceql_value(&between.low)?;
        let high = self.expr_to_traceql_value(&between.high)?;

        if between.negated {
            // NOT BETWEEN: value < low OR value > high
            Ok(format!(
                "({} < {} || {} > {})",
                attr_str, low, attr_str, high
            ))
        } else {
            // BETWEEN: value >= low AND value <= high
            Ok(format!("{} >= {} && {} <= {}", attr_str, low, attr_str, high))
        }
    }

    /// Map DataFusion operator to TraceQL operator string
    fn map_operator(&self, df_op: &DFOperator, reversed: bool) -> Result<&'static str, ConversionError> {
        let op = match df_op {
            DFOperator::Eq => "=",
            DFOperator::NotEq => "!=",
            DFOperator::Lt => {
                if reversed {
                    ">"
                } else {
                    "<"
                }
            }
            DFOperator::LtEq => {
                if reversed {
                    ">="
                } else {
                    "<="
                }
            }
            DFOperator::Gt => {
                if reversed {
                    "<"
                } else {
                    ">"
                }
            }
            DFOperator::GtEq => {
                if reversed {
                    "<="
                } else {
                    ">="
                }
            }
            DFOperator::RegexMatch => "=~",
            DFOperator::RegexNotMatch => "!~",
            _ => {
                return Err(ConversionError::UnsupportedExpression(format!(
                    "Operator {:?}",
                    df_op
                )))
            }
        };
        Ok(op)
    }

    /// Convert expression to TraceQL value string
    fn expr_to_traceql_value(&self, expr: &Expr) -> Result<String, ConversionError> {
        match expr {
            Expr::Literal(scalar) => self.scalar_to_traceql(scalar),
            _ => Err(ConversionError::UnsupportedExpression(
                "Non-literal value".into(),
            )),
        }
    }

    /// Convert ScalarValue to TraceQL value string
    fn scalar_to_traceql(&self, scalar: &ScalarValue) -> Result<String, ConversionError> {
        match scalar {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                Ok(format!("\"{}\"", escape_string(s)))
            }
            ScalarValue::Int8(Some(n)) => Ok(n.to_string()),
            ScalarValue::Int16(Some(n)) => Ok(n.to_string()),
            ScalarValue::Int32(Some(n)) => Ok(n.to_string()),
            ScalarValue::Int64(Some(n)) => Ok(n.to_string()),
            ScalarValue::UInt8(Some(n)) => Ok(n.to_string()),
            ScalarValue::UInt16(Some(n)) => Ok(n.to_string()),
            ScalarValue::UInt32(Some(n)) => Ok(n.to_string()),
            ScalarValue::UInt64(Some(n)) => Ok(n.to_string()),
            ScalarValue::Float32(Some(n)) => Ok(n.to_string()),
            ScalarValue::Float64(Some(n)) => Ok(n.to_string()),
            ScalarValue::Boolean(Some(b)) => Ok(if *b { "true" } else { "false" }.to_string()),
            ScalarValue::Null => Ok("nil".to_string()),
            _ => Err(ConversionError::UnsupportedExpression(format!(
                "Scalar {:?}",
                scalar
            ))),
        }
    }

    /// Check if expression is a column or map access
    fn is_attribute_expr(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::Column(_))
            || matches!(expr, Expr::ScalarFunction(f) if f.name() == "get_field")
    }

    /// Convert expression to TraceQLAttribute
    fn expr_to_attribute(&self, expr: &Expr) -> Result<Option<TraceQLAttribute>, ConversionError> {
        match expr {
            Expr::Column(col) => Ok(Some(self.column_to_attribute(&col.name)?)),
            Expr::ScalarFunction(func) if func.name() == "get_field" => {
                let map_col = self.extract_map_column_name(expr);
                let key = self.extract_map_access_key(expr);

                match (map_col.as_deref(), key) {
                    (Some("span_attributes"), Some(key)) => Ok(Some(TraceQLAttribute {
                        scope: AttributeScope::Span,
                        name: key,
                    })),
                    (Some("resource_attributes"), Some(key)) => Ok(Some(TraceQLAttribute {
                        scope: AttributeScope::Resource,
                        name: key,
                    })),
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    /// Extract map column name from get_field expression
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

    /// Extract key from map access expression
    fn extract_map_access_key(&self, expr: &Expr) -> Option<String> {
        if let Expr::ScalarFunction(func) = expr {
            if func.name() == "get_field" && func.args.len() == 2 {
                if let Expr::Literal(ScalarValue::Utf8(Some(key))) = &func.args[1] {
                    return Some(key.clone());
                }
            }
        }
        None
    }

    /// Map SQL column name to TraceQLAttribute
    fn column_to_attribute(&self, column_name: &str) -> Result<TraceQLAttribute, ConversionError> {
        // Check intrinsic mappings first
        if let Some(intrinsic) = self.column_mapping.get_intrinsic(column_name) {
            return Ok(TraceQLAttribute {
                scope: AttributeScope::Intrinsic(intrinsic.clone()),
                name: column_name.to_string(),
            });
        }

        // Check for resource attributes (resource_*)
        if let Some(attr_name) = column_name.strip_prefix("resource_") {
            return Ok(TraceQLAttribute {
                scope: AttributeScope::Resource,
                name: attr_name.replace('_', "."),
            });
        }

        // Default to span attribute
        Ok(TraceQLAttribute {
            scope: AttributeScope::Span,
            name: column_name.replace('_', "."),
        })
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
                    if let Some(&next) = chars.peek() {
                        if next == '%' || next == '_' {
                            regex.push(chars.next().unwrap());
                        } else {
                            regex.push_str("\\\\");
                        }
                    }
                }
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
}

/// Escape special characters in strings for TraceQL
fn escape_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: sql-to-traceql <SQL_QUERY>");
        eprintln!();
        eprintln!("Example:");
        eprintln!("  sql-to-traceql \"SELECT traceid FROM spans_view WHERE rootservicename = 'svc1'\"");
        std::process::exit(1);
    }

    let sql = args[1].clone();
    let converter = SqlToTraceQLConverter::new();
    
    match converter.convert(&sql).await {
        Ok(traceql) => {
            println!("{}", traceql);
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_simple_where() {
        let sql = "SELECT traceid FROM spans_view WHERE rootservicename = 'my-service'";
        let converter = SqlToTraceQLConverter::new();
        let result = converter.convert(sql).await.unwrap();
        assert_eq!(result, "{ rootServiceName = \"my-service\" }");
    }

    #[tokio::test]
    async fn test_and_conditions() {
        let sql = "SELECT traceid FROM spans_view WHERE rootservicename = 'svc1' AND http_status_code = 500";
        let converter = SqlToTraceQLConverter::new();
        let result = converter.convert(sql).await.unwrap();
        assert_eq!(result, "{ rootServiceName = \"svc1\" && span.http.status.code = 500 }");
    }

    #[tokio::test]
    async fn test_or_conditions() {
        let sql = "SELECT traceid FROM spans_view WHERE http_method = 'GET' OR http_method = 'POST'";
        let converter = SqlToTraceQLConverter::new();
        let result = converter.convert(sql).await.unwrap();
        assert_eq!(result, "{ (span.http.method = \"GET\" || span.http.method = \"POST\") }");
    }

    #[tokio::test]
    async fn test_complex_or_and() {
        let sql = "SELECT traceid FROM spans_view WHERE http_method = 'GET' OR (http_method = 'POST' AND http_status_code = 200)";
        let converter = SqlToTraceQLConverter::new();
        let result = converter.convert(sql).await.unwrap();
        assert!(result.contains("||"));
        assert!(result.contains("&&"));
    }

    #[tokio::test]
    async fn test_span_attributes() {
        let sql = "SELECT traceid FROM spans_view WHERE span_attributes['net.host.port'] = 8080";
        let converter = SqlToTraceQLConverter::new();
        let result = converter.convert(sql).await.unwrap();
        assert_eq!(result, "{ span.net.host.port = 8080 }");
    }

    #[tokio::test]
    async fn test_resource_attributes() {
        let sql = "SELECT traceid FROM spans_view WHERE resource_attributes['service.name'] = 'foo'";
        let converter = SqlToTraceQLConverter::new();
        let result = converter.convert(sql).await.unwrap();
        assert_eq!(result, "{ resource.service.name = \"foo\" }");
    }

    #[tokio::test]
    async fn test_no_where() {
        let sql = "SELECT traceid FROM spans_view";
        let converter = SqlToTraceQLConverter::new();
        let result = converter.convert(sql).await.unwrap();
        assert_eq!(result, "{ }");
    }

    #[tokio::test]
    async fn test_comparison_operators() {
        let sql = "SELECT traceid FROM spans_view WHERE http_status_code >= 400";
        let converter = SqlToTraceQLConverter::new();
        let result = converter.convert(sql).await.unwrap();
        assert_eq!(result, "{ span.http.status.code >= 400 }");
    }

    #[tokio::test]
    async fn test_in_list() {
        let sql = "SELECT traceid FROM spans_view WHERE http_method IN ('GET', 'POST')";
        let converter = SqlToTraceQLConverter::new();
        let result = converter.convert(sql).await.unwrap();
        assert!(result.contains("span.http.method = \"GET\""));
        assert!(result.contains("span.http.method = \"POST\""));
        assert!(result.contains("||"));
    }

    #[tokio::test]
    async fn test_select_with_attributes() {
        let sql = "SELECT traceid, span_attributes['custom.field'] FROM spans_view WHERE rootservicename = 'svc1'";
        let converter = SqlToTraceQLConverter::new();
        let result = converter.convert(sql).await.unwrap();
        assert!(result.contains("select(span.custom.field)"));
    }
}

