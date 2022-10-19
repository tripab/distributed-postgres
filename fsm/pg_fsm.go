package fsm

import (
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	pgquery "github.com/pganalyze/pg_query_go/v2"

	"github.com/tripab/distributed-postgres/queryengine"
	"github.com/tripab/distributed-postgres/snapshots"
)

type PgFsm struct {
	PE *queryengine.PgEngine
}

func (pf *PgFsm) Snapshot() (raft.FSMSnapshot, error) {
	return snapshots.SnapshotNoop{}, nil
}

func (pf *PgFsm) Restore(rc io.ReadCloser) error {
	return fmt.Errorf("nothing to restore")
}

func (pf *PgFsm) Apply(log *raft.Log) interface{} {
	switch log.Type {
	case raft.LogCommand:
		ast, err := pgquery.Parse(string(log.Data))
		if err != nil {
			panic(fmt.Errorf("could not parse payload: %s", err))
		}

		err = pf.PE.Execute(ast)
		if err != nil {
			panic(err)
		}
	default:
		panic(fmt.Errorf("unknown raft log type: %#v", log.Type))
	}

	return nil
}
