package wireprotocol

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/jackc/pgproto3/v2"
	pgquery "github.com/pganalyze/pg_query_go/v2"
	bolt "go.etcd.io/bbolt"

	"github.com/tripab/distributed-postgres/queryengine"
)

/*
 * Postgres wire protocol server
 */

type PgConn struct {
	Conn net.Conn
	Db   *bolt.DB
	R    *raft.Raft
}

func (pc PgConn) handleStartupMessage(PgConn *pgproto3.Backend) error {
	startupMessage, err := PgConn.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("error receiving startup message: %s", err)
	}

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err := pc.Conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error sending ready for query: %s", err)
		}

		return nil
	case *pgproto3.SSLRequest:
		_, err := pc.Conn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("error sending deny SSL request: %s", err)
		}

		return pc.handleStartupMessage(PgConn)
	default:
		return fmt.Errorf("unknown startup message: %#v", startupMessage)
	}
}

func (pc PgConn) handleMessage(pgc *pgproto3.Backend) error {
	msg, err := pgc.Receive()
	if err != nil {
		return fmt.Errorf("error receiving message: %s", err)
	}

	switch t := msg.(type) {
	case *pgproto3.Query:
		stmts, err := pgquery.Parse(t.String)
		if err != nil {
			return fmt.Errorf("error parsing query: %s", err)
		}

		if len(stmts.GetStmts()) > 1 {
			return fmt.Errorf("only make one request at a time")
		}

		stmt := stmts.GetStmts()[0]

		// Handle SELECTs here
		s := stmt.GetStmt().GetSelectStmt()
		if s != nil {
			pe := queryengine.NewPgEngine(pc.Db)
			res, err := pe.ExecuteSelect(s)
			if err != nil {
				return err
			}

			pc.writePgResult(res)
			return nil
		}

		// Otherwise it's DDL/DML, raftify
		future := pc.R.Apply([]byte(t.String), 500*time.Millisecond)
		if err := future.Error(); err != nil {
			return fmt.Errorf("could not apply: %s", err)
		}

		e := future.Response()
		if e != nil {
			return fmt.Errorf("could not apply (internal): %s", e)
		}

		pc.done(nil, strings.ToUpper(strings.Split(t.String, " ")[0])+" ok")
	case *pgproto3.Terminate:
		return nil
	default:
		return fmt.Errorf("received message other than Query from client: %s", msg)
	}

	return nil
}

func (pc PgConn) done(buf []byte, msg string) {
	buf = (&pgproto3.CommandComplete{CommandTag: []byte(msg)}).Encode(buf)
	buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	_, err := pc.Conn.Write(buf)
	if err != nil {
		log.Printf("Failed to write query response: %s", err)
	}
}

func (pc PgConn) handle() {
	pgc := pgproto3.NewBackend(pgproto3.NewChunkReader(pc.Conn), pc.Conn)
	defer pc.Conn.Close()

	err := pc.handleStartupMessage(pgc)
	if err != nil {
		log.Println(err)
		return
	}

	for {
		err := pc.handleMessage(pgc)
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func RunPgServer(port string, Db *bolt.DB, R *raft.Raft) {
	ln, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		log.Fatal(err)
	}

	for {
		Conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}

		pc := PgConn{Conn, Db, R}
		go pc.handle()
	}
}

var dataTypeOIDMap = map[string]uint32{
	"text":            25,
	"pg_catalog.int4": 23,
}

func (pc PgConn) writePgResult(res *queryengine.PgResult) {
	rd := &pgproto3.RowDescription{}
	for i, field := range res.FieldNames {
		rd.Fields = append(rd.Fields, pgproto3.FieldDescription{
			Name:        []byte(field),
			DataTypeOID: dataTypeOIDMap[res.FieldTypes[i]],
		})
	}
	buf := rd.Encode(nil)
	for _, row := range res.Rows {
		dr := &pgproto3.DataRow{}
		for _, value := range row {
			bs, err := json.Marshal(value)
			if err != nil {
				log.Printf("Failed to marshal cell: %s\n", err)
				return
			}
			dr.Values = append(dr.Values, bs)
		}
		buf = dr.Encode(buf)
	}

	pc.done(buf, fmt.Sprintf("SELECT %d", len(res.Rows)))
}
