// ATTENTION: This is a PoC implementation and should not be used for production.
// AI HAZARD WARNING: LLM was heavily used to produce this code and should not be trusted.
package traceql

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

type jsonFetchSpansRequest struct {
	StartTimeUnixNanos  uint64          `json:"start_time_unix_nanos"`
	EndTimeUnixNanos    uint64          `json:"end_time_unix_nanos"`
	Conditions          []jsonCondition `json:"conditions"`
	AllConditions       bool            `json:"all_conditions"`
	Limit               *int            `json:"limit,omitempty"`
	SecondPass          []jsonCondition `json:"second_pass_conditions,omitempty"`
	SecondPassSelectAll bool            `json:"second_pass_select_all,omitempty"`
	FilterExpression    *jsonFilterExpr `json:"filter_expression,omitempty"`
}

// ============================================================================
// Filter Expression Tree - for evaluating SQL WHERE clause on spans
// ============================================================================

// jsonFilterExpr represents a node in the filter expression tree.
// The Type field determines which other fields are used.
type jsonFilterExpr struct {
	Type string `json:"type"` // "and", "or", "not", "comparison", "exists", "true"

	// For "and" and "or" types
	Left  *jsonFilterExpr `json:"left,omitempty"`
	Right *jsonFilterExpr `json:"right,omitempty"`

	// For "not" type
	Expression *jsonFilterExpr `json:"expression,omitempty"`

	// For "comparison" type
	Attribute *jsonAttribute `json:"attribute,omitempty"`
	Op        string         `json:"op,omitempty"`
	Value     *jsonStatic    `json:"value,omitempty"`
}

// evaluateFilterExpr evaluates a filter expression against a span.
// Returns true if the span matches the filter.
func evaluateFilterExpr(expr *jsonFilterExpr, span Span) (bool, error) {
	if expr == nil {
		return true, nil
	}

	switch expr.Type {
	case "true":
		return true, nil

	case "and":
		leftResult, err := evaluateFilterExpr(expr.Left, span)
		if err != nil {
			return false, err
		}
		if !leftResult {
			return false, nil // short-circuit
		}
		return evaluateFilterExpr(expr.Right, span)

	case "or":
		leftResult, err := evaluateFilterExpr(expr.Left, span)
		if err != nil {
			return false, err
		}
		if leftResult {
			return true, nil // short-circuit
		}
		return evaluateFilterExpr(expr.Right, span)

	case "not":
		result, err := evaluateFilterExpr(expr.Expression, span)
		if err != nil {
			return false, err
		}
		return !result, nil

	case "exists":
		if expr.Attribute == nil {
			return true, nil
		}
		attr, err := convertJSONAttribute(*expr.Attribute)
		if err != nil {
			return false, err
		}
		_, exists := span.AttributeFor(attr)
		return exists, nil

	case "comparison":
		if expr.Attribute == nil || expr.Value == nil {
			return true, nil
		}
		return evaluateComparison(expr, span)

	default:
		return true, fmt.Errorf("unknown filter expression type: %s", expr.Type)
	}
}

// evaluateComparison evaluates a comparison expression against a span.
func evaluateComparison(expr *jsonFilterExpr, span Span) (bool, error) {
	attr, err := convertJSONAttribute(*expr.Attribute)
	if err != nil {
		return false, err
	}

	spanValue, exists := span.AttributeFor(attr)
	if !exists {
		// Attribute doesn't exist - comparison fails (except for != which succeeds)
		return expr.Op == "!=", nil
	}

	expectedValue, err := convertJSONStatic(*expr.Value)
	if err != nil {
		return false, err
	}

	switch expr.Op {
	case "=":
		return spanValue.Equals(&expectedValue), nil
	case "!=":
		return !spanValue.Equals(&expectedValue), nil
	case "<":
		return compareStatic(spanValue, expectedValue) < 0, nil
	case "<=":
		return compareStatic(spanValue, expectedValue) <= 0, nil
	case ">":
		return compareStatic(spanValue, expectedValue) > 0, nil
	case ">=":
		return compareStatic(spanValue, expectedValue) >= 0, nil
	case "=~": // regex match
		return matchRegex(spanValue, expectedValue)
	case "!~": // regex not match
		match, err := matchRegex(spanValue, expectedValue)
		return !match, err
	default:
		return true, fmt.Errorf("unknown comparison operator: %s", expr.Op)
	}
}

// compareStatic compares two static values.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func compareStatic(a, b Static) int {
	// Try numeric comparison first
	aFloat := a.Float()
	bFloat := b.Float()
	if aFloat < bFloat {
		return -1
	}
	if aFloat > bFloat {
		return 1
	}
	return 0
}

// matchRegex checks if a static value matches a regex pattern.
func matchRegex(value Static, pattern Static) (bool, error) {
	valueStr := value.EncodeToString(false)
	patternStr := pattern.EncodeToString(false)

	re, err := regexp.Compile(patternStr)
	if err != nil {
		return false, fmt.Errorf("invalid regex pattern %q: %w", patternStr, err)
	}

	return re.MatchString(valueStr), nil
}

// createFilterSecondPass creates a SecondPass function that filters spans
// using the provided filter expression.
func createFilterSecondPass(filterExpr *jsonFilterExpr) func(*Spanset) ([]*Spanset, error) {
	return func(inSS *Spanset) ([]*Spanset, error) {
		if len(inSS.Spans) == 0 {
			return nil, nil
		}

		// If no filter expression, return all spans
		if filterExpr == nil || filterExpr.Type == "true" {
			return []*Spanset{inSS}, nil
		}

		// Filter spans that match the expression
		var matchingSpans []Span
		for _, span := range inSS.Spans {
			matches, err := evaluateFilterExpr(filterExpr, span)
			if err != nil {
				return nil, fmt.Errorf("evaluating filter expression: %w", err)
			}
			if matches {
				matchingSpans = append(matchingSpans, span)
			}
		}

		if len(matchingSpans) == 0 {
			return nil, nil
		}

		// Return a new spanset with only the matching spans
		result := &Spanset{
			TraceID:            inSS.TraceID,
			RootSpanName:       inSS.RootSpanName,
			RootServiceName:    inSS.RootServiceName,
			StartTimeUnixNanos: inSS.StartTimeUnixNanos,
			DurationNanos:      inSS.DurationNanos,
			ServiceStats:       inSS.ServiceStats,
			Attributes:         inSS.Attributes,
			Spans:              matchingSpans,
		}

		return []*Spanset{result}, nil
	}
}

type jsonCondition struct {
	Attribute jsonAttribute `json:"attribute"`
	Op        string        `json:"op"`
	Operands  []jsonStatic  `json:"operands,omitempty"`
}

type jsonAttribute struct {
	Scope     string  `json:"scope"`
	Name      string  `json:"name"`
	Intrinsic *string `json:"intrinsic,omitempty"`
	Parent    bool    `json:"parent,omitempty"`
}

type jsonStatic struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

type spansetOutput struct {
	TraceID            string                   `json:"trace_id"`
	RootSpanName       string                   `json:"root_span_name,omitempty"`
	RootServiceName    string                   `json:"root_service_name,omitempty"`
	StartTimeUnixNanos uint64                   `json:"start_time_unix_nanos"`
	DurationNanos      uint64                   `json:"duration_nanos"`
	Spans              []spanOutput             `json:"spans"`
	ServiceStats       map[string]serviceStats  `json:"service_stats,omitempty"`
	Attributes         []spansetAttributeOutput `json:"attributes,omitempty"`
}

type spanOutput struct {
	SpanID             string            `json:"span_id"`
	StartTimeUnixNanos uint64            `json:"start_time_unix_nanos"`
	DurationNanos      uint64            `json:"duration_nanos"`
	Attributes         map[string]string `json:"attributes,omitempty"`
}

type serviceStats struct {
	SpanCount  uint32 `json:"span_count"`
	ErrorCount uint32 `json:"error_count"`
}

type spansetAttributeOutput struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func IsSQLQuery(query string) bool {
	// temp mock
	return strings.Contains(query, "SELECT")
}

func SQLToFetchSpansRequest(query string) (FetchSpansRequest, error) {
	return SQLToFetchSpansRequestHTTP(query)
}

var (
	translatorURL = os.Getenv("TRANSLATOR_URL")
	endpoint      = "convert"
)

func SQLToFetchSpansRequestHTTP(query string) (FetchSpansRequest, error) {
	if translatorURL == "" {
		return FetchSpansRequest{}, fmt.Errorf("TRANSLATOR_URL environment variable is not set")
	}

	u, err := url.Parse(translatorURL)
	if err != nil {
		return FetchSpansRequest{}, fmt.Errorf("parsing translator URL: %w", err)
	}
	u.Path = endpoint
	queryParams := url.Values{}
	queryParams.Add("q", query)
	u.RawQuery = queryParams.Encode()

	resp, err := http.Get(u.String())
	if err != nil {
		return FetchSpansRequest{}, fmt.Errorf("sending request to translator: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return FetchSpansRequest{}, fmt.Errorf("translator returned status %d", resp.StatusCode)
	}

	var jsonReq jsonFetchSpansRequest
	if err := json.NewDecoder(resp.Body).Decode(&jsonReq); err != nil {
		return FetchSpansRequest{}, fmt.Errorf("decoding translator response: %w", err)
	}

	return convertJSONToFetchSpansRequest(jsonReq)
}

// TODO: replace with external http call to my Rust script
func SQLToFetchSpansRequestHardcoded(query string) (FetchSpansRequest, error) {
	// mock implementation for now

	var jsonReq jsonFetchSpansRequest
	if err := json.Unmarshal([]byte(hardcoded), &jsonReq); err != nil {
		return FetchSpansRequest{}, fmt.Errorf("parsing request json: %w", err)
	}

	return convertJSONToFetchSpansRequest(jsonReq)
}

func convertJSONToFetchSpansRequest(j jsonFetchSpansRequest) (FetchSpansRequest, error) {
	req := FetchSpansRequest{
		StartTimeUnixNanos: j.StartTimeUnixNanos,
		EndTimeUnixNanos:   j.EndTimeUnixNanos,
		AllConditions:      j.AllConditions,
	}

	for _, jc := range j.Conditions {
		cond, err := convertJSONCondition(jc)
		if err != nil {
			return req, err
		}
		req.Conditions = append(req.Conditions, cond)
	}
	for _, jc := range j.SecondPass {
		cond, err := convertJSONCondition(jc)
		if err != nil {
			return req, err
		}
		req.SecondPassConditions = append(req.SecondPassConditions, cond)
	}
	req.SecondPassSelectAll = j.SecondPassSelectAll

	// Always run a second pass when metadata is requested so intrinsic fields
	// like trace/span IDs and timings are populated in the output.
	if len(req.SecondPassConditions) == 0 && !req.SecondPassSelectAll {
		req.SecondPassConditions = SearchMetaConditions()
	}

	// Use the filter expression from Rust to create the SecondPass function.
	// This evaluates the SQL WHERE clause on each span locally without
	// transferring span data over the network.
	if j.FilterExpression != nil {
		req.SecondPass = createFilterSecondPass(j.FilterExpression)
	} else if len(req.SecondPassConditions) > 0 || req.SecondPassSelectAll {
		// Fallback to pass-through if no filter expression
		req.SecondPass = func(s *Spanset) ([]*Spanset, error) {
			return []*Spanset{s}, nil
		}
	}
	return req, nil
}

func convertJSONCondition(jc jsonCondition) (Condition, error) {
	attr, err := convertJSONAttribute(jc.Attribute)
	if err != nil {
		return Condition{}, err
	}
	op, err := parseOperator(jc.Op)
	if err != nil {
		return Condition{}, err
	}

	cond := Condition{
		Attribute: attr,
		Op:        op,
	}
	for _, operand := range jc.Operands {
		static, err := convertJSONStatic(operand)
		if err != nil {
			return cond, err
		}
		cond.Operands = append(cond.Operands, static)
	}
	return cond, nil
}

func parseOperator(s string) (Operator, error) {
	switch s {
	case "none", "":
		return OpNone, nil
	case "=":
		return OpEqual, nil
	case "!=":
		return OpNotEqual, nil
	case "<":
		return OpLess, nil
	case "<=":
		return OpLessEqual, nil
	case ">":
		return OpGreater, nil
	case ">=":
		return OpGreaterEqual, nil
	case "=~":
		return OpRegex, nil
	case "!~":
		return OpNotRegex, nil
	default:
		return OpNone, fmt.Errorf("unknown operator: %s", s)
	}
}

func convertJSONStatic(js jsonStatic) (Static, error) {
	switch js.Type {
	case "string":
		if s, ok := js.Value.(string); ok {
			return NewStaticString(s), nil
		}
		return Static{}, fmt.Errorf("expected string value, got %T", js.Value)

	case "int":
		switch v := js.Value.(type) {
		case float64:
			return NewStaticInt(int(v)), nil
		case int:
			return NewStaticInt(v), nil
		case int64:
			return NewStaticInt(int(v)), nil
		default:
			return Static{}, fmt.Errorf("expected int value, got %T", js.Value)
		}

	case "float":
		if f, ok := js.Value.(float64); ok {
			return NewStaticFloat(f), nil
		}
		return Static{}, fmt.Errorf("expected float value, got %T", js.Value)

	case "boolean":
		if b, ok := js.Value.(bool); ok {
			return NewStaticBool(b), nil
		}
		return Static{}, fmt.Errorf("expected boolean value, got %T", js.Value)

	case "duration":
		switch v := js.Value.(type) {
		case float64:
			return NewStaticDuration(time.Duration(int64(v))), nil
		case string:
			d, err := time.ParseDuration(v)
			if err != nil {
				return Static{}, fmt.Errorf("parsing duration: %w", err)
			}
			return NewStaticDuration(d), nil
		default:
			return Static{}, fmt.Errorf("expected duration value, got %T", js.Value)
		}

	case "status":
		if s, ok := js.Value.(string); ok {
			status, err := parseStatus(s)
			if err != nil {
				return Static{}, fmt.Errorf("parsing status: %w", err)
			}
			return NewStaticStatus(status), nil
		}
		return Static{}, fmt.Errorf("expected status string, got %T", js.Value)

	case "kind":
		if s, ok := js.Value.(string); ok {
			kind, err := parseKind(s)
			if err != nil {
				return Static{}, fmt.Errorf("parsing kind: %w", err)
			}
			return NewStaticKind(kind), nil
		}
		return Static{}, fmt.Errorf("expected kind string, got %T", js.Value)

	default:
		return Static{}, fmt.Errorf("unknown static type: %s", js.Type)
	}
}

func parseStatus(s string) (Status, error) {
	switch s {
	case "error":
		return StatusError, nil
	case "ok":
		return StatusOk, nil
	case "unset":
		return StatusUnset, nil
	default:
		return StatusUnset, fmt.Errorf("unknown status: %s", s)
	}
}

func parseKind(s string) (Kind, error) {
	switch s {
	case "unspecified":
		return KindUnspecified, nil
	case "internal":
		return KindInternal, nil
	case "client":
		return KindClient, nil
	case "server":
		return KindServer, nil
	case "producer":
		return KindProducer, nil
	case "consumer":
		return KindConsumer, nil
	default:
		return KindUnspecified, fmt.Errorf("unknown kind: %s", s)
	}
}

func convertJSONAttribute(ja jsonAttribute) (Attribute, error) {
	attr := Attribute{
		Name:   ja.Name,
		Parent: ja.Parent,
	}

	switch ja.Scope {
	case "none", "":
		attr.Scope = AttributeScopeNone
	case "resource":
		attr.Scope = AttributeScopeResource
	case "span":
		attr.Scope = AttributeScopeSpan
	case "intrinsic":
		attr.Scope = AttributeScopeNone // Intrinsics set below
	default:
		return attr, fmt.Errorf("unknown attribute scope: %s", ja.Scope)
	}

	if ja.Intrinsic != nil {
		i, err := parseIntrinsic(*ja.Intrinsic)
		if err != nil {
			return attr, err
		}
		attr.Intrinsic = i
	}

	return attr, nil
}

func parseIntrinsic(s string) (Intrinsic, error) {
	switch s {
	case "trace:rootService":
		return IntrinsicTraceRootService, nil
	case "trace:rootSpan":
		return IntrinsicTraceRootSpan, nil
	case "trace:duration":
		return IntrinsicTraceDuration, nil
	case "trace:id":
		return IntrinsicTraceID, nil
	case "trace:start":
		return IntrinsicTraceStartTime, nil
	case "span:id":
		return IntrinsicSpanID, nil
	case "span:start":
		return IntrinsicSpanStartTime, nil
	case "duration":
		return IntrinsicDuration, nil
	case "name":
		return IntrinsicName, nil
	case "status":
		return IntrinsicStatus, nil
	case "kind":
		return IntrinsicKind, nil
	case "parent":
		return IntrinsicParent, nil
	case "", "none":
		return IntrinsicNone, nil
	default:
		return IntrinsicNone, fmt.Errorf("unknown intrinsic: %s", s)
	}
}

var hardcoded = `
{
  "start_time_unix_nanos": 0,
  "end_time_unix_nanos": 18446744073709551615,
  "conditions": [
    {
      "attribute": {
        "scope": "span",
        "name": "net.host.port",
        "parent": false
      },
      "op": "!=",
      "operands": [
        {
          "type": "int",
          "value": 8955
        }
      ]
    }
  ],
  "all_conditions": true,
  "limit": 50,
  "second_pass_conditions": [
    {
      "attribute": {
        "scope": "intrinsic",
        "name": "traceid",
        "intrinsic": "trace:id",
        "parent": false
      },
      "op": "none",
      "operands": []
    },
    {
      "attribute": {
        "scope": "span",
        "name": "net.host.name",
        "parent": false
      },
      "op": "none",
      "operands": []
    }
  ],
  "second_pass_select_all": false
}
`
