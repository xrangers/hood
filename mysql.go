package hood

import (
	"fmt"
	"reflect"
	"time"
_   "github.com/ziutek/mymysql/godrv"
)

func init() {
	RegisterDialect("mymysql", NewMysql())
}

type mysql struct {
	base
}

func NewMysql() Dialect {
	d := &mysql{}
	d.base.Dialect = d
	return d
}

func (d *mysql) NextMarker(pos *int) string {
	return "?"
}

func (d *mysql) Quote(s string) string {
	return fmt.Sprintf("`%s`", s)
}

func (d *mysql) ParseBool(value reflect.Value) bool {
	return value.Int() != 0
}

func (d *mysql) SqlType(f interface{}, size int) string {
	switch f.(type) {
	case Id:
		return "int"
	case time.Time, Created, Updated:
		return "timestamp"
	case bool:
		return "boolean"
	case int, int8, int16, int32, uint, uint8, uint16, uint32:
		return "int"
	case int64, uint64:
		return "bigint"
	case float32, float64:
		return "double"
	case []byte:
		if size > 0 && size < 65532 {
			return fmt.Sprintf("varbinary(%d)", size)
		}
		return "longblob"
	case string:
		if size > 0 && size < 65532 {
			return fmt.Sprintf("varchar(%d)", size)
		}
		return "text"
	}
	panic("invalid sql type")
}

func (d *mysql) KeywordAutoIncrement() string {
	return "AUTO_INCREMENT"
}

func (d *mysql) DropIndexSql(table_name, name string) string {
	return fmt.Sprintf("DROP INDEX %v on %v", d.Quote(name), d.Quote(table_name))
}

func (d *mysql) CreateTable(hood *Hood, model *Model) error {
	_, err := hood.Exec(d.CreateTableSql(model, false) + " CHARSET = 'utf8'")
	return err
}
