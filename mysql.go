package hood

import (
	"fmt"
	_ "github.com/ziutek/mymysql/godrv"
	"reflect"
	"strings"
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

func (d *mysql) CreateTableSql(model *Model, ifNotExists bool) string {
	a := []string{"CREATE TABLE "}
	if ifNotExists {
		a = append(a, "IF NOT EXISTS ")
	}
	a = append(a, d.Quote(model.Table), " ( ")
	for i, field := range model.Fields {
		b := []string{
			d.Quote(field.Name),
			d.SqlType(field.Value, field.Size()),
		}
		if field.NotNull() {
			b = append(b, d.KeywordNotNull())
		}
		if x := field.Default(); x != "" {
			b = append(b, d.KeywordDefault(x))
		}
		if field.PrimaryKey() {
			b = append(b, d.KeywordPrimaryKey())
		}
		if incKeyword := d.Dialect.KeywordAutoIncrement(); field.AutoIncr() && incKeyword != "" {
			b = append(b, incKeyword)
		}
		a = append(a, strings.Join(b, " "))
		if i < len(model.Fields)-1 {
			a = append(a, ", ")
		}
	}
	a = append(a, " )")
	return strings.Join(a, "")
}
