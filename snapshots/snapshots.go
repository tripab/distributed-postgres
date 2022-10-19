package snapshots

import (
	"github.com/hashicorp/raft"
)

type SnapshotNoop struct{}

func (sn SnapshotNoop) Persist(sink raft.SnapshotSink) error {
	return sink.Cancel()
}

func (sn SnapshotNoop) Release() {}
