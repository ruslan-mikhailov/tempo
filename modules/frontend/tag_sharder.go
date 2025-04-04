package frontend

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/tempo/modules/frontend/combiner"
	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/segmentio/fasthash/fnv1a"
)

/* tagsSearchRequest request interface for transform tags and tags V2 requests into a querier request */
type tagsSearchRequest struct {
	request tempopb.SearchTagsRequest
}

func (r *tagsSearchRequest) start() uint32 {
	return r.request.Start
}

func (r *tagsSearchRequest) end() uint32 {
	return r.request.End
}

func (r *tagsSearchRequest) hash() uint64 {
	return fnv1a.HashString64(r.request.Scope)
}

func (r *tagsSearchRequest) keyPrefix() string {
	return cacheKeyPrefixSearchTag
}

func (r *tagsSearchRequest) rf1After() time.Time { return r.request.RF1After }

func (r *tagsSearchRequest) newWithRange(start, end uint32) tagSearchReq {
	newReq := r.request
	newReq.Start = start
	newReq.End = end

	return &tagsSearchRequest{
		request: newReq,
	}
}

func (r *tagsSearchRequest) buildSearchTagRequest(subR *http.Request) (*http.Request, error) {
	return api.BuildSearchTagsRequest(subR, &r.request)
}

func (r *tagsSearchRequest) buildTagSearchBlockRequest(subR *http.Request, blockID string,
	startPage int, pages int, m *backend.BlockMeta,
) (*http.Request, error) {
	return api.BuildSearchTagsBlockRequest(subR, &tempopb.SearchTagsBlockRequest{
		BlockID:       blockID,
		StartPage:     uint32(startPage),
		PagesToSearch: uint32(pages),
		Encoding:      m.Encoding.String(),
		IndexPageSize: m.IndexPageSize,
		TotalRecords:  m.TotalRecords,
		DataEncoding:  m.DataEncoding,
		Version:       m.Version,
		Size_:         m.Size_,
		FooterSize:    m.FooterSize,
	})
}

/* TagValue V2 handler and request implementation */
type tagValueSearchRequest struct {
	request tempopb.SearchTagValuesRequest
}

func (r *tagValueSearchRequest) start() uint32 {
	return r.request.Start
}

func (r *tagValueSearchRequest) end() uint32 {
	return r.request.End
}

func (r *tagValueSearchRequest) hash() uint64 {
	hash := fnv1a.HashString64(r.request.TagName)
	hash = fnv1a.AddString64(hash, traceql.ExtractMatchers(r.request.Query))

	return hash
}

func (r *tagValueSearchRequest) keyPrefix() string {
	return cacheKeyPrefixSearchTagValues
}

func (r *tagValueSearchRequest) rf1After() time.Time { return r.request.RF1After }

func (r *tagValueSearchRequest) newWithRange(start, end uint32) tagSearchReq {
	newReq := r.request
	newReq.Start = start
	newReq.End = end

	return &tagValueSearchRequest{
		request: newReq,
	}
}

func (r *tagValueSearchRequest) buildSearchTagRequest(subR *http.Request) (*http.Request, error) {
	return api.BuildSearchTagValuesRequest(subR, &r.request)
}

func (r *tagValueSearchRequest) buildTagSearchBlockRequest(subR *http.Request, blockID string,
	startPage int, pages int, m *backend.BlockMeta,
) (*http.Request, error) {
	return api.BuildSearchTagValuesBlockRequest(subR, &tempopb.SearchTagValuesBlockRequest{
		BlockID:       blockID,
		StartPage:     uint32(startPage),
		PagesToSearch: uint32(pages),
		Encoding:      m.Encoding.String(),
		IndexPageSize: m.IndexPageSize,
		TotalRecords:  m.TotalRecords,
		DataEncoding:  m.DataEncoding,
		Version:       m.Version,
		Size_:         m.Size_,
		FooterSize:    m.FooterSize,
	})
}

func parseTagsRequest(r *http.Request) (tagSearchReq, error) {
	searchReq, err := api.ParseSearchTagsRequest(r)
	if err != nil {
		return nil, err
	}
	return &tagsSearchRequest{
		request: *searchReq,
	}, nil
}

func parseTagValuesRequest(r *http.Request) (tagSearchReq, error) {
	searchReq, err := api.ParseSearchTagValuesRequest(r)
	if err != nil {
		return nil, err
	}
	return &tagValueSearchRequest{
		request: *searchReq,
	}, nil
}

type parseRequestFunction func(r *http.Request) (tagSearchReq, error)

type tagSearchReq interface {
	start() uint32
	end() uint32
	newWithRange(start, end uint32) tagSearchReq
	buildSearchTagRequest(subR *http.Request) (*http.Request, error)
	buildTagSearchBlockRequest(*http.Request, string, int, int, *backend.BlockMeta) (*http.Request, error)

	// funcs for calculating cache keys. this hash should NOT use the start/end ranges of the request and
	// should only be based on the content the request is searching for
	hash() uint64
	keyPrefix() string

	rf1After() time.Time
}

type searchTagSharder struct {
	next      pipeline.AsyncRoundTripper[combiner.PipelineResponse]
	reader    tempodb.Reader
	overrides overrides.Interface

	cfg          SearchSharderConfig
	logger       log.Logger
	parseRequest parseRequestFunction
}

// newAsyncTagSharder creates a sharding middleware for tags and tag values
func newAsyncTagSharder(reader tempodb.Reader, o overrides.Interface, cfg SearchSharderConfig, parseRequest parseRequestFunction, logger log.Logger) pipeline.AsyncMiddleware[combiner.PipelineResponse] {
	return pipeline.AsyncMiddlewareFunc[combiner.PipelineResponse](func(next pipeline.AsyncRoundTripper[combiner.PipelineResponse]) pipeline.AsyncRoundTripper[combiner.PipelineResponse] {
		return searchTagSharder{
			next:         next,
			reader:       reader,
			overrides:    o,
			cfg:          cfg,
			logger:       logger,
			parseRequest: parseRequest,
		}
	})
}

// RoundTrip implements pipeline.AsyncRoundTripper
// execute up to concurrentRequests simultaneously where each request scans ~targetMBsPerRequest
// until limit results are found
func (s searchTagSharder) RoundTrip(pipelineRequest pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
	r := pipelineRequest.HTTPRequest()
	ctx := pipelineRequest.Context()

	tenantID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return pipeline.NewBadRequest(err), nil
	}

	searchReq, err := s.parseRequest(r)
	if err != nil {
		return pipeline.NewBadRequest(err), nil
	}
	ctx, span := tracer.Start(ctx, "frontend.ShardSearchTags")
	defer span.End()
	pipelineRequest.SetContext(ctx)

	// calculate and enforce max search duration
	maxDuration := s.maxDuration(tenantID)
	if maxDuration != 0 && time.Duration(searchReq.end()-searchReq.start())*time.Second > maxDuration {
		return pipeline.NewBadRequest(fmt.Errorf("range specified by start and end exceeds %s."+
			" received start=%d end=%d", maxDuration, searchReq.start(), searchReq.end())), nil
	}

	// build request to search ingester based on query_ingesters_until config and time range
	// pass subCtx in requests, so we can cancel and exit early
	ingesterReq, err := s.ingesterRequest(tenantID, pipelineRequest, searchReq)
	if err != nil {
		return nil, err
	}

	reqCh := make(chan pipeline.Request, 1) // buffer of 1 allows us to insert ingestReq if it exists
	if ingesterReq != nil {
		reqCh <- ingesterReq
	}

	s.backendRequests(ctx, tenantID, pipelineRequest, searchReq, reqCh, func(err error) {
		// todo: actually find a way to return this error to the user
		s.logger.Log("msg", "failed to build backend requests", "err", err)
	})

	// TODO(suraj): send jobMetricsResponse like we send in asyncSearchSharder.RoundTrip and accumulate these metrics in the
	// combiners, and log these metrics in the logger like we do in search_handlers.go

	// execute requests
	return pipeline.NewAsyncSharderChan(ctx, s.cfg.ConcurrentRequests, reqCh, nil, s.next), nil
}

// backendRequest builds backend requests to search backend blocks. backendRequest takes ownership of reqCh and closes it.
// it returns 3 int values: totalBlocks, totalBlockBytes, and estimated jobs
func (s searchTagSharder) backendRequests(ctx context.Context, tenantID string, parent pipeline.Request, searchReq tagSearchReq, reqCh chan<- pipeline.Request, errFn func(error)) {
	// request without start or end, search only in ingester
	if searchReq.start() == 0 || searchReq.end() == 0 {
		close(reqCh)
		return
	}

	// calculate duration (start and end) to search the backend blocks
	start, end := backendRange(searchReq.start(), searchReq.end(), s.cfg.QueryBackendAfter)

	// no need to search backend
	if start == end {
		close(reqCh)
		return
	}

	rf1After := searchReq.rf1After()
	if rf1After.IsZero() {
		rf1After = s.cfg.RF1After
	}

	// get block metadata of blocks in start, end duration
	startT := time.Unix(int64(start), 0)
	endT := time.Unix(int64(end), 0)
	blocks := blockMetasForSearch(s.reader.BlockMetas(tenantID), startT, endT, rf1FilterFn(rf1After))

	targetBytesPerRequest := s.cfg.TargetBytesPerRequest

	go func() {
		s.buildBackendRequests(ctx, tenantID, parent, blocks, targetBytesPerRequest, reqCh, errFn, searchReq)
	}()
}

// buildBackendRequests returns a slice of requests that cover all blocks in the store
// that are covered by start/end.
func (s searchTagSharder) buildBackendRequests(ctx context.Context, tenantID string, parent pipeline.Request, metas []*backend.BlockMeta, bytesPerRequest int, reqCh chan<- pipeline.Request, errFn func(error), searchReq tagSearchReq) {
	defer close(reqCh)

	hash := searchReq.hash()
	keyPrefix := searchReq.keyPrefix()
	startTime := time.Unix(int64(searchReq.start()), 0)
	endTime := time.Unix(int64(searchReq.end()), 0)

	for _, m := range metas {
		pages := pagesPerRequest(m, bytesPerRequest)
		if pages == 0 {
			continue
		}

		blockID := m.BlockID.String()
		for startPage := 0; startPage < int(m.TotalRecords); startPage += pages {
			pipelineR, err := cloneRequestforQueriers(parent, tenantID, func(r *http.Request) (*http.Request, error) {
				return searchReq.buildTagSearchBlockRequest(r, blockID, startPage, pages, m)
			})
			if err != nil {
				errFn(err)
				continue
			}

			key := cacheKey(keyPrefix, tenantID, hash, startTime, endTime, m, startPage, pages)
			pipelineR.SetCacheKey(key)

			select {
			case reqCh <- pipelineR:
			case <-ctx.Done():
				return
			}
		}
	}
}

// ingesterRequest returns a new start and end time range for the backend as well as a http request
// that covers the ingesters. If nil is returned for the http.Request then there is no ingesters query.
// we should do a copy of the searchReq before use this function, as it is an interface, we cannot guaranteed  be passed
// by value.
func (s searchTagSharder) ingesterRequest(tenantID string, parent pipeline.Request, searchReq tagSearchReq) (pipeline.Request, error) {
	// request without start or end, search only in ingester
	if searchReq.start() == 0 || searchReq.end() == 0 {
		return s.buildIngesterRequest(tenantID, parent, searchReq)
	}

	now := time.Now()
	ingesterUntil := uint32(now.Add(-s.cfg.QueryIngestersUntil).Unix())

	// if there's no overlap between the query and ingester range just return nil
	if searchReq.end() < ingesterUntil {
		return nil, nil
	}

	ingesterStart := searchReq.start()
	ingesterEnd := searchReq.end()

	// adjust ingesterStart if necessary
	if ingesterStart < ingesterUntil {
		ingesterStart = ingesterUntil
	}

	// if ingester start == ingester end then we don't need to query it
	if ingesterStart == ingesterEnd {
		return nil, nil
	}

	newSearchReq := searchReq.newWithRange(ingesterStart, ingesterEnd)
	return s.buildIngesterRequest(tenantID, parent, newSearchReq)
}

func (s searchTagSharder) buildIngesterRequest(tenantID string, parent pipeline.Request, searchReq tagSearchReq) (pipeline.Request, error) {
	subR, err := cloneRequestforQueriers(parent, tenantID, func(r *http.Request) (*http.Request, error) {
		return searchReq.buildSearchTagRequest(r)
	})
	if err != nil {
		return nil, err
	}
	return subR, nil
}

// maxDuration returns the max search duration allowed for this tenant.
func (s searchTagSharder) maxDuration(tenantID string) time.Duration {
	// check overrides first, if no overrides then grab from our config
	maxDuration := s.overrides.MaxSearchDuration(tenantID)
	if maxDuration != 0 {
		return maxDuration
	}

	return s.cfg.MaxDuration
}
