package collector

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assetStatusServer routes the two GraphQL queries CollectAssetStatus issues
// (get_asset_nodes.graphql, then get_assets_latest_info.graphql) to separate
// canned response bodies, keyed off which field name the request's query
// text asks for — the same shape as a real Dagster endpoint, which answers
// both from a single /graphql URL.
func assetStatusServer(t *testing.T, nodesBody, latestInfoBody string) *httptest.Server {
	t.Helper()
	return assetStatusServerWithSteps(t, nodesBody, latestInfoBody, `{"data": {"runsOrError": {"__typename": "Runs", "results": []}}}`)
}

// assetStatusServerWithSteps also answers the third query
// (get_run_step_stats.graphql), sent for failed and canceled runs.
func assetStatusServerWithSteps(t *testing.T, nodesBody, latestInfoBody, stepStatsBody string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req GraphQLRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		switch {
		case strings.Contains(req.Query, "assetNodes"):
			_, err := w.Write([]byte(nodesBody))
			require.NoError(t, err)
		case strings.Contains(req.Query, "assetsLatestInfo"):
			_, err := w.Write([]byte(latestInfoBody))
			require.NoError(t, err)
		case strings.Contains(req.Query, "runsOrError"):
			_, err := w.Write([]byte(stepStatsBody))
			require.NoError(t, err)
		default:
			t.Fatalf("unexpected query: %s", req.Query)
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

// TestCollectAssetStatusDistinguishesNeverRunFromFailed exercises the reason
// this collector needs a second query at all: assetNodes.staleStatus is
// derived from assetMaterializations, which only records successful events,
// so a failed run and "never run" both show up there as MISSING. Only
// assetsLatestInfo.latestRun.status tells them apart (issue #56).
func TestCollectAssetStatusDistinguishesNeverRunFromFailed(t *testing.T) {
	nodesBody := `{
		"data": {
			"assetNodes": [
				{"assetKey": {"path": ["good_asset"]}, "staleStatus": "FRESH"},
				{"assetKey": {"path": ["bad_asset"]}, "staleStatus": "MISSING"},
				{"assetKey": {"path": ["never_run_asset"]}, "staleStatus": "MISSING"}
			]
		}
	}`
	latestInfoBody := `{
		"data": {
			"assetsLatestInfo": [
				{"assetKey": {"path": ["good_asset"]}, "latestRun": {"status": "SUCCESS", "endTime": 1700000100}},
				{"assetKey": {"path": ["bad_asset"]}, "latestRun": {"status": "FAILURE", "endTime": 1700000200}},
				{"assetKey": {"path": ["never_run_asset"]}, "latestRun": null}
			]
		}
	}`
	ts := assetStatusServer(t, nodesBody, latestInfoBody)
	c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectAssetStatus(t.Context(), c))

	require.Len(t, c.assetStatus, 3)
	assert.Equal(t, assetStatusEntry{staleStatus: "FRESH", lastMaterializationStatus: "SUCCESS", lastMaterializationTimestamp: 1700000100, hasLastMaterializationTimestamp: true}, c.assetStatus["good_asset"])
	assert.Equal(t, assetStatusEntry{staleStatus: "MISSING", lastMaterializationStatus: "FAILURE", lastMaterializationTimestamp: 1700000200, hasLastMaterializationTimestamp: true}, c.assetStatus["bad_asset"],
		"a failed run must be distinguishable from an asset that has never run, even though both look MISSING via staleStatus alone")
	assert.Equal(t, assetStatusEntry{staleStatus: "MISSING", lastMaterializationStatus: ""}, c.assetStatus["never_run_asset"])
}

// TestCollectAssetStatusJoinsMultiSegmentAssetKeys checks the "/"-joined
// label value for an asset key with more than one path segment (e.g. a dbt
// project and model name), the common case in practice.
func TestCollectAssetStatusJoinsMultiSegmentAssetKeys(t *testing.T) {
	nodesBody := `{
		"data": {
			"assetNodes": [
				{"assetKey": {"path": ["my_dbt_project", "customers"]}, "staleStatus": "STALE"}
			]
		}
	}`
	latestInfoBody := `{
		"data": {
			"assetsLatestInfo": [
				{"assetKey": {"path": ["my_dbt_project", "customers"]}, "latestRun": {"status": "SUCCESS"}}
			]
		}
	}`
	ts := assetStatusServer(t, nodesBody, latestInfoBody)
	c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectAssetStatus(t.Context(), c))

	require.Contains(t, c.assetStatus, "my_dbt_project/customers")
}

// TestCollectAssetStatusHandlesNullStaleStatus checks the nullable
// staleStatus field: an asset with no computable stale status must end up
// with an empty staleStatus, not a literal "null" or "<nil>" label value.
func TestCollectAssetStatusHandlesNullStaleStatus(t *testing.T) {
	nodesBody := `{
		"data": {
			"assetNodes": [
				{"assetKey": {"path": ["no_stale_status_asset"]}, "staleStatus": null}
			]
		}
	}`
	latestInfoBody := `{
		"data": {
			"assetsLatestInfo": [
				{"assetKey": {"path": ["no_stale_status_asset"]}, "latestRun": null}
			]
		}
	}`
	ts := assetStatusServer(t, nodesBody, latestInfoBody)
	c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectAssetStatus(t.Context(), c))

	require.Contains(t, c.assetStatus, "no_stale_status_asset")
	assert.Equal(t, "", c.assetStatus["no_stale_status_asset"].staleStatus)
}

func TestCollectAssetStatusLeavesOutStaleStatusOfPartitionedAssets(t *testing.T) {
	// Dagster answers FRESH for a partitioned asset asked without a partition,
	// whatever its partitions are, so the status must not be reported.
	nodesBody := `{
		"data": {
			"assetNodes": [
				{"assetKey": {"path": ["partitioned_asset"]}, "isPartitioned": true, "staleStatus": "FRESH"},
				{"assetKey": {"path": ["plain_asset"]}, "isPartitioned": false, "staleStatus": "STALE"}
			]
		}
	}`
	latestInfoBody := `{
		"data": {
			"assetsLatestInfo": [
				{"assetKey": {"path": ["partitioned_asset"]}, "latestRun": {"status": "SUCCESS", "endTime": 100}},
				{"assetKey": {"path": ["plain_asset"]}, "latestRun": {"status": "SUCCESS", "endTime": 100}}
			]
		}
	}`
	ts := assetStatusServer(t, nodesBody, latestInfoBody)
	c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectAssetStatus(t.Context(), c))

	assert.Equal(t, "", c.assetStatus["partitioned_asset"].staleStatus)
	assert.Equal(t, "STALE", c.assetStatus["plain_asset"].staleStatus)
	assert.Equal(t, "SUCCESS", c.assetStatus["partitioned_asset"].lastMaterializationStatus,
		"the run-based metrics still cover a partitioned asset")
}

// TestCollectAssetStatusSkipsLatestInfoQueryWhenNoAssetsDefined checks that
// an empty assetNodes result short-circuits before asking assetsLatestInfo
// for zero asset keys — a request the schema doesn't reject, but that's
// pure waste when the answer is already known to be empty.
func TestCollectAssetStatusSkipsLatestInfoQueryWhenNoAssetsDefined(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req GraphQLRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		if strings.Contains(req.Query, "assetsLatestInfo") {
			t.Fatal("assetsLatestInfo should not be queried when assetNodes returned nothing")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"data": {"assetNodes": []}}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectAssetStatus(t.Context(), c))
	assert.Empty(t, c.assetStatus)
}

// TestCollectAssetStatusReturnsErrorOnGraphQLError checks that a top-level
// GraphQL error from the first query is propagated rather than treated as
// "no assets".
func TestCollectAssetStatusReturnsErrorOnGraphQLError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"errors": [{"message": "boom"}]}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)

	err := CollectAssetStatus(t.Context(), c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestReflectAssetStatus(t *testing.T) {
	c := NewDagsterCollector(t.Context(), "http://unused", "", time.Hour, time.Hour, 500, 5*time.Minute)
	c.assetStatus = map[string]assetStatusEntry{
		"good_asset":      {staleStatus: "FRESH", lastMaterializationStatus: "SUCCESS", lastMaterializationTimestamp: 1700000100, hasLastMaterializationTimestamp: true},
		"never_run_asset": {staleStatus: "MISSING", lastMaterializationStatus: ""},
	}

	ch := make(chan prometheus.Metric, 16)
	go func() {
		reflectAssetStatus(c, ch)
		close(ch)
	}()

	type key struct {
		metric   string
		assetKey string
		status   string
	}
	seen := make(map[key]float64)
	for m := range ch {
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))

		desc := m.Desc().String()
		var metricName string
		switch {
		case strings.Contains(desc, "dagster_asset_stale_status"):
			metricName = "dagster_asset_stale_status"
		case strings.Contains(desc, "dagster_asset_last_materialization_status"):
			metricName = "dagster_asset_last_materialization_status"
		case strings.Contains(desc, "dagster_asset_last_materialization_timestamp_seconds"):
			metricName = "dagster_asset_last_materialization_timestamp_seconds"
		default:
			t.Fatalf("unexpected metric desc: %s", desc)
		}

		var assetKey, status string
		for _, l := range dm.GetLabel() {
			switch l.GetName() {
			case "asset_key":
				assetKey = l.GetValue()
			case "status":
				status = l.GetValue()
			}
		}
		seen[key{metricName, assetKey, status}] = dm.GetGauge().GetValue()
	}

	assert.Equal(t, float64(1), seen[key{"dagster_asset_stale_status", "good_asset", "fresh"}])
	assert.Equal(t, float64(1), seen[key{"dagster_asset_last_materialization_status", "good_asset", "success"}])
	assert.Equal(t, float64(1700000100), seen[key{"dagster_asset_last_materialization_timestamp_seconds", "good_asset", ""}])
	assert.Equal(t, float64(1), seen[key{"dagster_asset_stale_status", "never_run_asset", "missing"}])
	assert.Len(t, seen, 4,
		"never_run_asset must not emit dagster_asset_last_materialization_status or dagster_asset_last_materialization_timestamp_seconds: it has no run to report")
}

// TestCollectAssetStatusCarriesTimestampWhileRunIsInProgress covers issue
// #156: while an asset's latest run is still in progress, assetsLatestInfo
// returns endTime: null. That must not become a 0 timestamp (1970). Instead
// the previous scrape's end time is kept, and with no previous value (the
// exporter started mid-run) the timestamp is left unset.
func TestCollectAssetStatusCarriesTimestampWhileRunIsInProgress(t *testing.T) {
	const nodesBody = `{"data": {"assetNodes": [{"assetKey": {"path": ["asset_a"]}, "staleStatus": "FRESH"}]}}`
	latestInfo := func(latestRun string) string {
		return `{"data": {"assetsLatestInfo": [{"assetKey": {"path": ["asset_a"]}, "latestRun": ` + latestRun + `}]}}`
	}
	const (
		finished = `{"status": "SUCCESS", "endTime": 1700000100}`
		running  = `{"status": "STARTED", "endTime": null}`
	)

	var current atomic.Value
	current.Store(latestInfo(running))
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req GraphQLRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		body := nodesBody
		if strings.Contains(req.Query, "assetsLatestInfo") {
			body = current.Load().(string)
		}
		_, err := w.Write([]byte(body))
		require.NoError(t, err)
	}))
	t.Cleanup(ts.Close)

	c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)
	scrape := func(latestRun string) assetStatusEntry {
		t.Helper()
		current.Store(latestInfo(latestRun))
		require.NoError(t, CollectAssetStatus(t.Context(), c))
		return c.assetStatus["asset_a"]
	}

	entry := scrape(running)
	assert.Equal(t, "STARTED", entry.lastMaterializationStatus)
	assert.False(t, entry.hasLastMaterializationTimestamp,
		"a run already in progress when the exporter starts has no end time to report yet")

	entry = scrape(finished)
	assert.True(t, entry.hasLastMaterializationTimestamp)
	assert.Equal(t, float64(1700000100), entry.lastMaterializationTimestamp)

	entry = scrape(running)
	assert.Equal(t, "STARTED", entry.lastMaterializationStatus)
	assert.True(t, entry.hasLastMaterializationTimestamp)
	assert.Equal(t, float64(1700000100), entry.lastMaterializationTimestamp,
		"while the next run is in progress, the previous run's end time should be kept, not reset to 0")
}

func TestCollectAssetStatusWithStaleStatusDisabledDoesNotAskForIt(t *testing.T) {
	var queries []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req GraphQLRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		queries = append(queries, req.Query)

		w.Header().Set("Content-Type", "application/json")
		body := `{"data":{"assetNodes":[{"assetKey":{"path":["plain_asset"]}}]}}`
		if strings.Contains(req.Query, "assetsLatestInfo") {
			body = `{"data":{"assetsLatestInfo":[{"assetKey":{"path":["plain_asset"]},"latestRun":{"status":"FAILURE","endTime":100}}]}}`
		}
		if strings.Contains(req.Query, "runsOrError") {
			body = `{"data":{"runsOrError":{"__typename":"Runs","results":[]}}}`
		}
		_, err := w.Write([]byte(body))
		require.NoError(t, err)
	}))
	t.Cleanup(ts.Close)
	c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)
	c.DisableAssetStaleStatus()

	require.NoError(t, CollectAssetStatus(t.Context(), c))

	require.Len(t, queries, 3, "asset nodes, latest runs, and the failed run's steps")
	assert.NotContains(t, queries[0], "staleStatus", "the query that makes Dagster compute it must not be sent")
	assert.Contains(t, queries[0], "GetAssetKeys")
	entry := c.assetStatus["plain_asset"]
	assert.Equal(t, "", entry.staleStatus)
	assert.Equal(t, "FAILURE", entry.lastMaterializationStatus, "the run-based metrics are unaffected")
	assert.True(t, entry.hasLastMaterializationTimestamp)
}

// TestCollectAssetStatusReportsAnAssetBySteps covers a run that several
// assets shared (declarative automation launches them together): one failed
// step fails the run, but only the asset whose step failed reports FAILURE.
// The run's steps are asked for once, however many assets it ran.
func TestCollectAssetStatusReportsAnAssetBySteps(t *testing.T) {
	nodesBody := `{
		"data": {
			"assetNodes": [
				{"assetKey": {"path": ["fine"]}, "opNames": ["fine"]},
				{"assetKey": {"path": ["broken"]}, "opNames": ["broken"]},
				{"assetKey": {"path": ["mapped"]}, "opNames": ["graph.mapped"]},
				{"assetKey": {"path": ["half_mapped"]}, "opNames": ["half_mapped"]},
				{"assetKey": {"path": ["not_started"]}, "opNames": ["not_started"]},
				{"assetKey": {"path": ["cancelled_after_its_step"]}, "opNames": ["cancelled_after_its_step"]}
			]
		}
	}`
	latestInfoBody := `{
		"data": {
			"assetsLatestInfo": [
				{"assetKey": {"path": ["fine"]}, "latestRun": {"runId": "shared", "status": "FAILURE", "endTime": 100}},
				{"assetKey": {"path": ["broken"]}, "latestRun": {"runId": "shared", "status": "FAILURE", "endTime": 100}},
				{"assetKey": {"path": ["mapped"]}, "latestRun": {"runId": "shared", "status": "FAILURE", "endTime": 100}},
				{"assetKey": {"path": ["half_mapped"]}, "latestRun": {"runId": "shared", "status": "FAILURE", "endTime": 100}},
				{"assetKey": {"path": ["not_started"]}, "latestRun": {"runId": "shared", "status": "FAILURE", "endTime": 100}},
				{"assetKey": {"path": ["cancelled_after_its_step"]}, "latestRun": {"runId": "cancelled", "status": "CANCELED", "endTime": 200}}
			]
		}
	}`
	stepStatsBody := `{
		"data": {
			"runsOrError": {
				"__typename": "Runs",
				"results": [
					{"runId": "shared", "stepStats": [
						{"stepKey": "fine", "status": "SUCCESS"},
						{"stepKey": "broken", "status": "FAILURE"},
						{"stepKey": "graph.mapped[a]", "status": "SUCCESS"},
						{"stepKey": "graph.mapped[b]", "status": "SUCCESS"},
						{"stepKey": "half_mapped[a]", "status": "SUCCESS"},
						{"stepKey": "half_mapped[b]", "status": "FAILURE"},
						{"stepKey": "fine_but_another_op", "status": "FAILURE"}
					]},
					{"runId": "cancelled", "stepStats": [
						{"stepKey": "cancelled_after_its_step", "status": "SUCCESS"}
					]}
				]
			}
		}
	}`
	var stepQueries []map[string]interface{}
	inner := assetStatusServerWithSteps(t, nodesBody, latestInfoBody, stepStatsBody)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req GraphQLRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		if strings.Contains(req.Query, "runsOrError") {
			stepQueries = append(stepQueries, req.Variables)
		}
		body, err := json.Marshal(req)
		require.NoError(t, err)
		resp, err := http.Post(inner.URL, "application/json", strings.NewReader(string(body)))
		require.NoError(t, err)
		defer func() { require.NoError(t, resp.Body.Close()) }()
		w.Header().Set("Content-Type", "application/json")
		_, err = io.Copy(w, resp.Body)
		require.NoError(t, err)
	}))
	t.Cleanup(ts.Close)
	c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectAssetStatus(t.Context(), c))

	require.Len(t, stepQueries, 1)
	assert.ElementsMatch(t, []interface{}{"cancelled", "shared"}, stepQueries[0]["runIds"])
	status := func(key string) string { return c.assetStatus[key].lastMaterializationStatus }
	assert.Equal(t, "SUCCESS", status("fine"), "its own step succeeded; another asset's step failed the run")
	assert.Equal(t, "FAILURE", status("broken"))
	assert.Equal(t, "SUCCESS", status("mapped"), "every mapped step of a graph-backed op succeeded")
	assert.Equal(t, "FAILURE", status("half_mapped"), "one of its mapped steps failed")
	assert.Equal(t, "FAILURE", status("not_started"), "no step of its own in the run: the run's status")
	assert.Equal(t, "SUCCESS", status("cancelled_after_its_step"))
	assert.Equal(t, 100.0, c.assetStatus["fine"].lastMaterializationTimestamp, "the end time is still the run's")
}

// TestCollectAssetStatusFallsBackToTheRunWhenItsStepsCannotBeRead checks
// that a failing step query costs only the refinement: the assets keep
// their runs' statuses and the scrape succeeds.
func TestCollectAssetStatusFallsBackToTheRunWhenItsStepsCannotBeRead(t *testing.T) {
	nodesBody := `{"data": {"assetNodes": [{"assetKey": {"path": ["fine"]}, "opNames": ["fine"]}]}}`
	latestInfoBody := `{"data": {"assetsLatestInfo": [{"assetKey": {"path": ["fine"]}, "latestRun": {"runId": "r", "status": "FAILURE", "endTime": 100}}]}}`
	stepStatsBody := `{"data": {"runsOrError": {"__typename": "PythonError", "message": "boom"}}}`
	ts := assetStatusServerWithSteps(t, nodesBody, latestInfoBody, stepStatsBody)
	c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectAssetStatus(t.Context(), c))

	assert.Equal(t, "FAILURE", c.assetStatus["fine"].lastMaterializationStatus)
}
