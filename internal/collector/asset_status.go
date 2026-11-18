package collector

import (
	"context"
	"log"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// assetStatusEntry is one asset's most recently observed state.
type assetStatusEntry struct {
	// staleStatus is "" when Dagster reports no stale status for this asset
	// (the field is nullable in the schema) — no dagster_asset_stale_status
	// series is emitted for it.
	staleStatus string
	// lastMaterializationStatus is "" when the asset has never had a run —
	// no dagster_asset_last_materialization_status series is emitted for it.
	lastMaterializationStatus string
	// lastMaterializationTimestamp is the end time of the asset's most recent
	// run that has finished. It's only meaningful when
	// hasLastMaterializationTimestamp is set: while the latest run is still
	// in progress its endTime is null, and the previous scrape's value is
	// carried over instead (issue #156). With nothing to carry over (the
	// exporter started mid-run), no dagster_asset_last_materialization_timestamp_seconds
	// series is emitted until the run finishes.
	lastMaterializationTimestamp    float64
	hasLastMaterializationTimestamp bool
}

// CollectAssetStatus reports each asset's staleness and the outcome of its
// most recent materializing run.
//
// This is two GraphQL round trips rather than one: assetNodes (every asset
// currently defined, across every code location, in one fetch — like
// repositoriesOrError but without a repositorySelector or per-location
// union) has to run first, because assetsLatestInfo needs the asset keys it
// returns as input. Neither field is reachable via repositoriesOrError, so
// this is its own collector — the same reasoning as
// CollectCodeLocationStatus and CollectDaemonHealth.
//
// assetNodes.staleStatus alone can't answer "did the last run succeed":
// staleStatus is derived from assetMaterializations, which only ever
// records successful events. A run that fails before producing a
// materialization leaves an asset looking exactly like one that has simply
// never run (see issue #56's investigation notes). Only
// assetsLatestInfo.latestRun.status distinguishes the two, hence the second
// query.
//
// A run's status is not always its assets' outcome, though: declarative
// automation launches many assets in one run, and one failed step fails the
// whole run. So for the failed and canceled runs among the latest runs, a
// third query reads their steps (runsOrError by run ID, stepStats), and an
// asset whose own steps (opNames) all succeeded reports SUCCESS. An asset
// with no step in the run (never started, or a renamed op) keeps the run's
// status.
func CollectAssetStatus(ctx context.Context, c *DagsterCollector) error {
	nodesReq := getAssetNodesRequest(!c.assetStaleStatusDisabled)
	nodesResp, err := getAssetNodes(ctx, nodesReq, c.client)
	if err != nil {
		log.Printf("failed to collect asset nodes from dagster: %v", err)
		return err
	}

	entries := make(map[string]assetStatusEntry, len(nodesResp.Data.AssetNodes))
	assetKeys := make([]AssetKeyPath, 0, len(nodesResp.Data.AssetNodes))
	opNames := make(map[string][]string, len(nodesResp.Data.AssetNodes))
	for _, node := range nodesResp.Data.AssetNodes {
		opNames[assetKeyLabel(node.AssetKey.Path)] = node.OpNames
		var entry assetStatusEntry
		// Dagster answers a partitioned asset's staleStatus with FRESH when no
		// partition is given, whatever its partitions' state is. Emitting
		// that would claim a freshness nobody computed, so it is left out.
		// Per-partition status (staleStatusByPartition) would be one series
		// per partition, which is too many for assets with many of them.
		if node.StaleStatus != nil && !node.IsPartitioned {
			entry.staleStatus = *node.StaleStatus
		}
		entries[assetKeyLabel(node.AssetKey.Path)] = entry
		assetKeys = append(assetKeys, node.AssetKey.Path)
	}

	// The previous scrape's entries, for carrying a finished run's end time
	// over while the next run is in progress. CollectAssetStatus replaces
	// c.assetStatus wholesale rather than mutating it, so reading the old map
	// after releasing the lock is safe.
	c.mutex.Lock()
	previous := c.assetStatus
	c.mutex.Unlock()

	// No assets defined anywhere: skip the second query rather than ask
	// assetsLatestInfo for an empty key list.
	if len(assetKeys) > 0 {
		infoReq := getAssetsLatestInfoRequest(assetKeys)
		infoResp, err := getAssetsLatestInfo(ctx, infoReq, c.client)
		if err != nil {
			log.Printf("failed to collect assets latest info from dagster: %v", err)
			return err
		}

		// The failed and canceled latest runs, each asked for once however
		// many assets it ran.
		unsuccessfulRuns := map[string]bool{}
		for _, info := range infoResp.Data.AssetsLatestInfo {
			if info.LatestRun != nil && isUnsuccessfulRunStatus(info.LatestRun.Status) {
				unsuccessfulRuns[info.LatestRun.RunID] = true
			}
		}
		steps := runSteps(ctx, c, unsuccessfulRuns)

		for _, info := range infoResp.Data.AssetsLatestInfo {
			if info.LatestRun == nil {
				continue
			}
			key := assetKeyLabel(info.AssetKey.Path)
			entry := entries[key]
			entry.lastMaterializationStatus = info.LatestRun.Status
			if isUnsuccessfulRunStatus(info.LatestRun.Status) && ownStepsSucceeded(steps[info.LatestRun.RunID], opNames[key]) {
				entry.lastMaterializationStatus = "SUCCESS"
			}
			if info.LatestRun.EndTime != nil {
				entry.lastMaterializationTimestamp = *info.LatestRun.EndTime
				entry.hasLastMaterializationTimestamp = true
			} else if prev, ok := previous[key]; ok && prev.hasLastMaterializationTimestamp {
				entry.lastMaterializationTimestamp = prev.lastMaterializationTimestamp
				entry.hasLastMaterializationTimestamp = true
			}
			entries[key] = entry
		}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.assetStatus = entries

	return nil
}

func isUnsuccessfulRunStatus(status string) bool {
	return status == "FAILURE" || status == "CANCELED"
}

// runSteps answers each run's step outcomes by step key. When the query
// fails, it logs and answers none, so every asset keeps its run's status:
// the refinement is lost for one scrape, the run-based metric is not.
func runSteps(ctx context.Context, c *DagsterCollector, runIDs map[string]bool) map[string]map[string]string {
	steps := map[string]map[string]string{}
	if len(runIDs) == 0 {
		return steps
	}
	ids := make([]string, 0, len(runIDs))
	for id := range runIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	resp, err := getRunStepStats(ctx, getRunStepStatsRequest(ids), c.client)
	if err != nil {
		log.Printf("failed to collect the steps of failed runs from dagster, asset statuses fall back to their runs': %v", err)
		return steps
	}
	for _, run := range resp.Data.RunsOrError.Results {
		byKey := make(map[string]string, len(run.StepStats))
		for _, step := range run.StepStats {
			byKey[step.StepKey] = step.Status
		}
		steps[run.RunID] = byKey
	}
	return steps
}

// ownStepsSucceeded reports whether the run has at least one step of the
// asset's ops and all of them succeeded. A step's key is its op's node
// handle, with "[mapping key]" appended for a dynamically mapped step.
func ownStepsSucceeded(steps map[string]string, opNames []string) bool {
	found := false
	for stepKey, status := range steps {
		for _, op := range opNames {
			if stepKey != op && !strings.HasPrefix(stepKey, op+"[") {
				continue
			}
			if status != "SUCCESS" {
				return false
			}
			found = true
		}
	}
	return found
}

// assetKeyLabel joins an asset key's path segments into a single Prometheus
// label value, e.g. ["my_dbt_project", "customers"] becomes
// "my_dbt_project/customers" — the same separator Dagster's own UI uses to
// render an asset key.
func assetKeyLabel(path AssetKeyPath) string {
	return strings.Join(path, "/")
}

// reflectAssetStatus emits dagster_asset_stale_status,
// dagster_asset_last_materialization_status, and
// dagster_asset_last_materialization_timestamp_seconds from a single locked
// pass over c.assetStatus, the same reasoning as reflectDaemonHealth: all
// three come from one entry per asset.
func reflectAssetStatus(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for assetKey, entry := range c.assetStatus {
		if entry.staleStatus != "" {
			ch <- prometheus.MustNewConstMetric(
				c.assetStaleStatusDesc,
				prometheus.GaugeValue,
				1,
				assetKey,
				strings.ToLower(entry.staleStatus),
			)
		}

		if entry.lastMaterializationStatus != "" {
			ch <- prometheus.MustNewConstMetric(
				c.assetLastMaterializationStatusDesc,
				prometheus.GaugeValue,
				1,
				assetKey,
				strings.ToLower(entry.lastMaterializationStatus),
			)
		}

		if entry.hasLastMaterializationTimestamp {
			ch <- prometheus.MustNewConstMetric(
				c.assetLastMaterializationTimestampDesc,
				prometheus.GaugeValue,
				entry.lastMaterializationTimestamp,
				assetKey,
			)
		}
	}
}
