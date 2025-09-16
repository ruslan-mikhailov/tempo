package livestore

import (
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/tempo/pkg/util"
)

type PreparePartitionDownscaleResponse struct {
	Timestamp int64  `json:"timestamp"`
	State     string `json:"state"`
}

// PreparePartitionDownscaleHandler prepares the live-store's partition downscaling. The partition owned by the
// live-store will switch to INACTIVE state (read-only).
//
// Following methods are supported:
//
//   - GET
//     Returns timestamp when partition was switched to INACTIVE state, or 0, if partition is not in INACTIVE state.
//
//   - POST
//     Switches the partition to INACTIVE state (if not yet), and returns the timestamp when the switch to
//     INACTIVE state happened.
//
//   - DELETE
//     Sets partition back from INACTIVE to ACTIVE state.
func (s *LiveStore) PreparePartitionDownscaleHandler(w http.ResponseWriter, r *http.Request) {
	logger := log.With(s.logger, "partition", s.ingestPartitionID)

	// Don't allow callers to change the shutdown configuration while we're in the middle
	// of starting or shutting down.
	if s.State() != services.Running {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if s.ingestPartitionLifecycler == nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	switch r.Method {
	case http.MethodPost:
		// It's not allowed to prepare the downscale while in PENDING state. Why? Because if the downscale
		// will be later cancelled, we don't know if it was requested in PENDING or ACTIVE state, so we
		// don't know to which state reverting back. Given a partition is expected to stay in PENDING state
		// for a short period, we simply don't allow this case.
		state, _, err := s.ingestPartitionLifecycler.GetPartitionState(r.Context())
		if err != nil {
			level.Error(logger).Log("msg", "cannot downscale: failed to check partition state in the ring", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if state == ring.PartitionPending {
			level.Warn(logger).Log("msg", "received a request to prepare partition for shutdown, but the request can't be satisfied because the partition is in PENDING state")
			w.WriteHeader(http.StatusConflict)
			return
		}
		if state == ring.PartitionInactive {
			level.Info(logger).Log("msg", "partition is already set to INACTIVE state")
			break
		}

		if err := s.ingestPartitionLifecycler.ChangePartitionState(r.Context(), ring.PartitionInactive); err != nil {
			level.Error(logger).Log("msg", "failed to change partition state to inactive", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		level.Info(logger).Log("msg", "partition prepared for downscaling")

	case http.MethodDelete:
		state, _, err := s.ingestPartitionLifecycler.GetPartitionState(r.Context())
		if err != nil {
			level.Error(logger).Log("msg", "cannot cancel downscaling: failed to check partition state in the ring", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// If partition is inactive, make it active. We ignore other states Active and especially Pending.
		if state == ring.PartitionInactive {
			// We don't switch it back to PENDING state if there are not enough owners because we want to guarantee consistency
			// in the read path. If the partition is within the lookback period we need to guarantee that partition will be queried.
			// Moving back to PENDING will cause us loosing consistency, because PENDING partitions are not queried by design.
			// We could move back to PENDING if there are not enough owners and the partition moved to INACTIVE more than
			// "lookback period" ago, but since we delete inactive partitions with no owners that moved to inactive since longer
			// than "lookback period" ago, it looks to be an edge case not worth to address.
			if err := s.ingestPartitionLifecycler.ChangePartitionState(r.Context(), ring.PartitionActive); err != nil {
				level.Error(logger).Log("msg", "failed to change partition state to active", "err", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			level.Info(logger).Log("msg", "partition downscaling preparation cancelled")
		} else {
			level.Info(logger).Log("msg", "partition is not in INACTIVE state, so no need to cancel downscaling", "state", state.String())
		}
	case http.MethodGet:
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	state, stateTimestamp, err := s.ingestPartitionLifecycler.GetPartitionState(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", "failed to check partition state in the ring", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if state == ring.PartitionInactive {
		util.WriteJSONResponse(w, PreparePartitionDownscaleResponse{
			Timestamp: stateTimestamp.Unix(),
			State:     state.String(),
		})
	} else {
		util.WriteJSONResponse(w, PreparePartitionDownscaleResponse{
			Timestamp: 0,
			State:     state.String(),
		})
	}
}

// PrepareShutdownHandler prepares the live-store for shutdown by changing the ring state
// to stop accepting writes.
//
// Following methods are supported:
//
//   - GET
//     Returns the current prepare shutdown state.
//
//   - POST
//     Enables prepare shutdown mode (sets ring to read-only).
//
//   - DELETE
//     Disables prepare shutdown mode (sets ring back to read-write).
func (s *LiveStore) PrepareShutdownHandler(w http.ResponseWriter, r *http.Request) {
	// Don't allow callers to change the shutdown configuration while we're in the middle
	// of starting or shutting down.
	if s.State() != services.Running {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	switch r.Method {
	case http.MethodPost:
		s.setPrepareShutdown()
		level.Info(s.logger).Log("msg", "live-store prepared for shutdown")

	case http.MethodDelete:
		s.unsetPrepareShutdown()
		level.Info(s.logger).Log("msg", "live-store shutdown preparation cancelled")

	case http.MethodGet:
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	prepared := s.isPreparedForShutdown()
	util.WriteJSONResponse(w, map[string]any{"prepared": prepared})
}

// setPrepareShutdown toggles live-store lifecycler config to prepare for shutdown
func (s *LiveStore) setPrepareShutdown() {
	s.livestoreLifecycler.SetUnregisterOnShutdown(true)
	s.livestoreLifecycler.SetFlushOnShutdown(true)
	s.livestoreLifecycler.SetKeepInstanceInTheRingOnShutdown(false)

	if s.ingestPartitionLifecycler != nil {
		// When the prepare shutdown endpoint is called there are two changes in the partitions ring behavior:
		//
		// 1. If setPrepareShutdown() is called at startup, because of the shutdown marker found on disk,
		//    the live-store shouldn't create the partition if doesn't exist, because we expect the live-store will
		//    be scaled down shortly after.
		// 2. When the live-store will shutdown we'll have to remove the live-store from the partition owners,
		//    because we expect the live-store to be scaled down.
		s.ingestPartitionLifecycler.SetCreatePartitionOnStartup(false)
		s.ingestPartitionLifecycler.SetRemoveOwnerOnShutdown(true)
	}
}

// unsetPrepareShutdown resets live-store lifecycler config back to normal operation
func (s *LiveStore) unsetPrepareShutdown() {
	if s.ingestPartitionLifecycler != nil {
		s.ingestPartitionLifecycler.SetCreatePartitionOnStartup(true)
		s.ingestPartitionLifecycler.SetRemoveOwnerOnShutdown(false)
	}
}

// isPreparedForShutdown returns true if the live-store is prepared for shutdown
func (s *LiveStore) isPreparedForShutdown() bool {
	return shutdownMarker
}
