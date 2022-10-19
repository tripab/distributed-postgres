package queryengine

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	pgquery "github.com/pganalyze/pg_query_go/v2"
	bolt "go.etcd.io/bbolt"
)

/*
 * Query Engine
 */

type PgEngine struct {
	db         *bolt.DB
	bucketName []byte
}

func NewPgEngine(db *bolt.DB) *PgEngine {
	return &PgEngine{db, []byte("data")}
}

func (pe *PgEngine) Execute(tree *pgquery.ParseResult) error {
	for _, stmt := range tree.GetStmts() {
		n := stmt.GetStmt()
		if c := n.GetCreateStmt(); c != nil {
			return pe.ExecuteCreate(c)
		} else if c := n.GetInsertStmt(); c != nil {
			return pe.ExecuteInsert(c)
		} else if c := n.GetSelectStmt(); c != nil {
			_, err := pe.ExecuteSelect(c)
			return err
		} else {
			return fmt.Errorf("unknown statement type: %s", stmt)
		}
	}

	return nil
}

type tableDefinition struct {
	Name        string
	ColumnNames []string
	ColumnTypes []string
}

func (pe *PgEngine) ExecuteCreate(stmt *pgquery.CreateStmt) error {
	tbl := tableDefinition{}
	tbl.Name = stmt.Relation.Relname

	for _, c := range stmt.TableElts {
		cd := c.GetColumnDef()

		tbl.ColumnNames = append(tbl.ColumnNames, cd.Colname)

		// Names is namespaced. So `INT` is pg_catalog.int4. `BIGINT` is pg_catalog.int8.
		var columnType string
		for _, n := range cd.TypeName.Names {
			if columnType != "" {
				columnType += "."
			}
			columnType += n.GetString_().Str
		}
		tbl.ColumnTypes = append(tbl.ColumnTypes, columnType)
	}

	tableBytes, err := json.Marshal(tbl)
	if err != nil {
		return fmt.Errorf("could not marshal table: %s", err)
	}

	err = pe.db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(pe.bucketName)
		if err != nil {
			return err
		}
		return bkt.Put([]byte("tables_"+tbl.Name), tableBytes)
	})

	if err != nil {
		return fmt.Errorf("could not set key-value: %s", err)
	}

	return nil
}

func (pe *PgEngine) getTableDefinition(name string) (*tableDefinition, error) {
	var tbl tableDefinition

	err := pe.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pe.bucketName)
		if bkt == nil {
			return fmt.Errorf("table does not exist")
		}

		valBytes := bkt.Get([]byte("tables_" + name))
		err := json.Unmarshal(valBytes, &tbl)
		if err != nil {
			return fmt.Errorf("could not unmarshal table: %s", err)
		}

		return nil
	})

	return &tbl, err
}

func (pe *PgEngine) ExecuteInsert(stmt *pgquery.InsertStmt) error {
	tblName := stmt.Relation.Relname

	slct := stmt.GetSelectStmt().GetSelectStmt()
	for _, values := range slct.ValuesLists {
		var rowData []any
		for _, value := range values.GetList().Items {
			if c := value.GetAConst(); c != nil {
				if s := c.Val.GetString_(); s != nil {
					rowData = append(rowData, s.Str)
					continue
				}

				if i := c.Val.GetInteger(); i != nil {
					rowData = append(rowData, i.Ival)
					continue
				}
			}

			return fmt.Errorf("unknown value type: %s", value)
		}

		rowBytes, err := json.Marshal(rowData)
		if err != nil {
			return fmt.Errorf("could not marshal row: %s", err)
		}

		id := uuid.New().String()
		err = pe.db.Update(func(tx *bolt.Tx) error {
			bkt, err := tx.CreateBucketIfNotExists(pe.bucketName)
			if err != nil {
				return err
			}

			return bkt.Put([]byte("rows_"+tblName+"_"+id), rowBytes)
		})
		if err != nil {
			return fmt.Errorf("could not store row: %s", err)
		}
	}

	return nil
}

type PgResult struct {
	FieldNames []string
	FieldTypes []string
	Rows       [][]any
}

func (pe *PgEngine) ExecuteSelect(stmt *pgquery.SelectStmt) (*PgResult, error) {
	tblName := stmt.FromClause[0].GetRangeVar().Relname
	tbl, err := pe.getTableDefinition(tblName)
	if err != nil {
		return nil, err
	}

	results := &PgResult{}
	for _, c := range stmt.GetTargetList() {
		fieldName := c.GetResTarget().Val.GetColumnRef().Fields[0].GetString_().Str
		results.FieldNames = append(results.FieldNames, fieldName)

		fieldType := ""
		for i, cn := range tbl.ColumnNames {
			if cn == fieldName {
				fieldType = tbl.ColumnTypes[i]
			}
		}

		if fieldType == "" {
			return nil, fmt.Errorf("unknown field: %s", fieldName)
		}

		results.FieldTypes = append(results.FieldTypes, fieldType)
	}

	prefix := []byte("rows_" + tblName + "_")
	pe.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(pe.bucketName).Cursor()

		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var row []any
			err := json.Unmarshal(v, &row)
			if err != nil {
				return fmt.Errorf("unable to unmarshal row: %s", err)
			}

			var targetRow []any
			for _, target := range results.FieldNames {
				for i, field := range tbl.ColumnNames {
					if target == field {
						targetRow = append(targetRow, row[i])
					}
				}
			}

			results.Rows = append(results.Rows, targetRow)
		}

		return nil
	})

	return results, nil
}

func (pe *PgEngine) Delete() error {
	return pe.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pe.bucketName)
		if bkt != nil {
			return tx.DeleteBucket(pe.bucketName)
		}

		return nil
	})
}
