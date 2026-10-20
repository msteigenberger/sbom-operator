package dtrack

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dtrack "github.com/DependencyTrack/client-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	openEndMarker       = ".."
	maxPropertyValueLen = 255
)

// StageInterval represents a single ISO-8601 time interval (<Start>/<End>).
// If End == nil, the deployment is currently active in this stage (<Start>/..).
type StageInterval struct {
	Start time.Time
	End   *time.Time
}

// ParseStageIntervals parses a comma-separated ISO-8601 interval string, e.g.:
// "2026-10-01T10:00:00Z/2026-10-15T14:30:00Z,2026-10-20T08:00:00Z/.."
func ParseStageIntervals(raw string) ([]StageInterval, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	parts := strings.Split(raw, ",")
	intervals := make([]StageInterval, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		bounds := strings.SplitN(part, "/", 2)
		start, err := time.Parse(time.RFC3339, strings.TrimSpace(bounds[0]))
		if err != nil {
			return nil, fmt.Errorf("invalid interval start %q: %w", bounds[0], err)
		}

		var endPtr *time.Time
		if len(bounds) == 2 {
			endStr := strings.TrimSpace(bounds[1])
			if endStr != "" && endStr != openEndMarker {
				end, err := time.Parse(time.RFC3339, endStr)
				if err != nil {
					return nil, fmt.Errorf("invalid interval end %q: %w", endStr, err)
				}
				endUTC := end.UTC()
				endPtr = &endUTC
			}
		}

		intervals = append(intervals, StageInterval{
			Start: start.UTC(),
			End:   endPtr,
		})
	}

	return intervals, nil
}

// FormatStageIntervals serializes the intervals back into ISO-8601 format
// and ensures that Dependency-Track's 255-character limit is respected.
func FormatStageIntervals(intervals []StageInterval) string {
	if len(intervals) == 0 {
		return ""
	}

	formatSlice := func(slice []StageInterval) string {
		strs := make([]string, 0, len(slice))
		for _, iv := range slice {
			startStr := iv.Start.UTC().Format(time.RFC3339)
			if iv.End == nil {
				strs = append(strs, startStr+"/"+openEndMarker)
			} else {
				strs = append(strs, startStr+"/"+iv.End.UTC().Format(time.RFC3339))
			}
		}
		return strings.Join(strs, ",")
	}

	res := formatSlice(intervals)
	// Prevent VARCHAR(255) overflow in case of frequent rollbacks:
	// Always retain the very first interval (first_deployed_at) and the most recent intervals.
	for len(res) > maxPropertyValueLen && len(intervals) > 2 {
		intervals = append(intervals[:1], intervals[2:]...)
		res = formatSlice(intervals)
	}
	return res
}

// OpenStageInterval opens a new active interval if the project is not currently
// active in this stage (initial deployment or rollback).
// If it is already active (last interval is open-ended), it remains unchanged on pod restarts
// (or adopts an earlier start date if an existing ReplicaSet is older).
func OpenStageInterval(existingRaw string, candidateStart time.Time) string {
	if candidateStart.IsZero() {
		candidateStart = time.Now().UTC()
	} else {
		candidateStart = candidateStart.UTC()
	}

	intervals, err := ParseStageIntervals(existingRaw)
	if err != nil || len(intervals) == 0 {
		return FormatStageIntervals([]StageInterval{{Start: candidateStart, End: nil}})
	}

	last := &intervals[len(intervals)-1]
	if last.End == nil {
		// Project is already marked as active in this stage -> do not open a new interval.
		// If candidateStart is older (e.g., older ReplicaSet in a second namespace), adjust start time:
		if candidateStart.Before(last.Start) {
			last.Start = candidateStart
		}
		return FormatStageIntervals(intervals)
	}

	// Last interval was closed -> this is a rollback! Append a new open-ended interval:
	intervals = append(intervals, StageInterval{
		Start: candidateStart,
		End:   nil,
	})
	return FormatStageIntervals(intervals)
}

// CloseStageInterval closes the currently open interval when deactivating a project version.
func CloseStageInterval(existingRaw string, deactivatedAt time.Time) string {
	if deactivatedAt.IsZero() {
		deactivatedAt = time.Now().UTC()
	} else {
		deactivatedAt = deactivatedAt.UTC()
	}

	intervals, err := ParseStageIntervals(existingRaw)
	if err != nil || len(intervals) == 0 {
		return existingRaw
	}

	last := &intervals[len(intervals)-1]
	if last.End == nil {
		if deactivatedAt.Before(last.Start) {
			deactivatedAt = last.Start
		}
		last.End = &deactivatedAt
	}
	return FormatStageIntervals(intervals)
}

// IsActiveAt checks whether the project was active in this stage at timestamp t
// (can be used for historical trend calculations).
func IsActiveAt(intervals []StageInterval, t time.Time) bool {
	for _, iv := range intervals {
		if t.Before(iv.Start) {
			continue
		}
		if iv.End == nil || !t.After(*iv.End) {
			return true
		}
	}
	return false
}

func TestParseStageIntervals_EdgeCasesAndErrors(t *testing.T) {
	// Empty string and whitespace-only parts
	ivs, err := ParseStageIntervals("   ")
	assert.NoError(t, err)
	assert.Nil(t, ivs)

	ivs, err = ParseStageIntervals("2026-10-01T10:00:00Z/.. , , ")
	assert.NoError(t, err)
	assert.Len(t, ivs, 1)

	// Invalid start timestamp
	_, err = ParseStageIntervals("not-a-timestamp/..")
	assert.Error(t, err)

	// Invalid end timestamp
	_, err = ParseStageIntervals("2026-10-01T10:00:00Z/not-a-timestamp")
	assert.Error(t, err)

	// Older candidateStart adjusts existing open interval start
	earlier := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	updated := OpenStageInterval("2026-10-01T10:00:00Z/..", earlier)
	assert.Equal(t, "2026-09-01T00:00:00Z/..", updated)

	// CloseStageInterval with deactivatedAt before start clamps to start
	clamped := CloseStageInterval("2026-10-01T10:00:00Z/..", earlier)
	assert.Equal(t, "2026-10-01T10:00:00Z/2026-10-01T10:00:00Z", clamped)
}

func TestSyncAndCloseStageIntervalProperty_Lifecycle(t *testing.T) {
	projectUUID := uuid.New()
	var currentProps []dtrack.ProjectProperty
	var createCalls, updateCalls int

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		propPath := "/api/v1/project/" + projectUUID.String() + "/property"

		if r.URL.Path == propPath {
			switch r.Method {
			case http.MethodGet:
				_ = json.NewEncoder(w).Encode(currentProps)
				return
			case http.MethodPut:
				createCalls++
				var p dtrack.ProjectProperty
				_ = json.NewDecoder(r.Body).Decode(&p)
				currentProps = append(currentProps, p)
				_ = json.NewEncoder(w).Encode(p)
				return
			case http.MethodPost:
				updateCalls++
				var p dtrack.ProjectProperty
				_ = json.NewDecoder(r.Body).Decode(&p)
				for i := range currentProps {
					if currentProps[i].Group == p.Group && currentProps[i].Name == p.Name {
						currentProps[i].Value = p.Value
					}
				}
				_ = json.NewEncoder(w).Encode(p)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	g := NewDependencyTrackTarget(ts.URL, "apikey", "", "", "", "", "cluster-a", "tag", "", "", "", true, true, "", false)
	g.deploymentTimestampsEnabled = true
	require.NoError(t, g.Initialize())

	client, err := dtrack.NewClient(ts.URL, dtrack.WithAPIKey("apikey"))
	require.NoError(t, err)

	proj := dtrack.Project{UUID: projectUUID, Name: "app", Version: "1.0", Active: true}
	tRelease := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	// 1. Initial sync creates property with open interval
	g.syncStageIntervalProperty(context.Background(), client, proj, "cluster-a", false, tRelease, tRelease)
	assert.Equal(t, 1, createCalls)
	assert.Equal(t, "2026-10-01T10:00:00Z/..", currentProps[0].Value)

	// 2. Second sync with identical timestamp is a no-op (no extra update call)
	g.syncStageIntervalProperty(context.Background(), client, proj, "cluster-a", true, tRelease, tRelease)
	assert.Equal(t, 0, updateCalls)

	// 3. Closing interval updates the property
	err = g.closeStageInterval(context.Background(), "cluster-a", client, &proj)
	assert.NoError(t, err)
	assert.Equal(t, 1, updateCalls)
	assert.False(t, strings.HasSuffix(currentProps[0].Value, "/.."))
}
