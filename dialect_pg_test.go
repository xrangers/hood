package hood

import (
	"database/sql"
	_ "github.com/bmizerany/pq"
	"testing"
)

const (
	disableLiveTests = true
)

type PgDialectModel struct {
	Prim   int    `pk:"true"auto:"true"`
	First  string `null:"true"`
	Last   string `default:"'defaultValue'"`
	Amount int
}

func setupDb(t *testing.T) *Hood {
	db, err := sql.Open("postgres", "user=hood dbname=hood_test sslmode=disable")
	if err != nil {
		t.Fatal("could not open db", err)
	}
	hood := New(db, &DialectPg{})
	hood.Log = true

	return hood
}

func TestPgSaveAndDelete(t *testing.T) {
	if disableLiveTests {
		return
	}
	hood := setupDb(t)

	type pgSaveModel struct {
		Id Id
		A  string
		B  int
	}
	model1 := &pgSaveModel{
		A: "banana",
		B: 5,
	}
	model2 := &pgSaveModel{
		A: "orange",
		B: 4,
	}

	hood.DropTable(model1)

	err := hood.CreateTable(model1)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	id, err := hood.Save(model1)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if id != 1 {
		t.Fatal("wrong id", id)
	}

	model1.A = "grape"
	model1.B = 9

	id, err = hood.Save(model1)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if id != 1 {
		t.Fatal("wrong id", id)
	}

	id, err = hood.Save(model2)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if id != 2 {
		t.Fatal("wrong id", id)
	}
	if model2.Id != id {
		t.Fatal("id should have been copied", model2.Id)
	}

	id2, err := hood.Delete(model2)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if id != id2 {
		t.Fatal("wrong id", id, id2)
	}
}

func TestPgFind(t *testing.T) {
	if disableLiveTests {
		return
	}
	hood := setupDb(t)

	type pgFindModel struct {
		Id Id
		A  string
		B  int
		C  int8
		D  int16
		E  int32
		F  int64
		G  uint
		H  uint8
		I  uint16
		J  uint32
		K  uint64
		L  float32
		M  float64
		N  []byte
	}
	model1 := &pgFindModel{
		A: "string!",
		B: -1,
		C: -2,
		D: -3,
		E: -4,
		F: -5,
		G: 6,
		H: 7,
		I: 8,
		J: 9,
		K: 10,
		L: 11.5,
		M: 12.6,
		N: []byte("bytes!"),
	}

	hood.DropTable(model1)

	err := hood.CreateTable(model1)
	if err != nil {
		t.Fatal("error not nil", err)
	}

	var out []pgFindModel
	err = hood.Where("a = ? AND j = ?", "string!", 9).Find(&out)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if out != nil {
		t.Fatal("output should be nil", out)
	}

	id, err := hood.Save(model1)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if id != 1 {
		t.Fatal("wrong id", id)
	}

	err = hood.Where("a = ? AND j = ?", "string!", 9).Find(&out)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if out == nil {
		t.Fatal("output should not be nil")
	}
	if x := len(out); x != 1 {
		t.Fatal("invalid output length", x)
	}
	for _, v := range out {
		if x := v.Id; x != 1 {
			t.Fatal("invalid value", x)
		}
		if x := v.A; x != "string!" {
			t.Fatal("invalid value", x)
		}
		if x := v.B; x != -1 {
			t.Fatal("invalid value", x)
		}
		if x := v.C; x != -2 {
			t.Fatal("invalid value", x)
		}
		if x := v.D; x != -3 {
			t.Fatal("invalid value", x)
		}
		if x := v.E; x != -4 {
			t.Fatal("invalid value", x)
		}
		if x := v.F; x != -5 {
			t.Fatal("invalid value", x)
		}
		if x := v.G; x != 6 {
			t.Fatal("invalid value", x)
		}
		if x := v.H; x != 7 {
			t.Fatal("invalid value", x)
		}
		if x := v.I; x != 8 {
			t.Fatal("invalid value", x)
		}
		if x := v.J; x != 9 {
			t.Fatal("invalid value", x)
		}
		if x := v.K; x != 10 {
			t.Fatal("invalid value", x)
		}
		if x := v.L; x != 11.5 {
			t.Fatal("invalid value", x)
		}
		if x := v.M; x != 12.6 {
			t.Fatal("invalid value", x)
		}
		if x := v.N; string(x) != "bytes!" {
			t.Fatal("invalid value", x)
		}
	}

	model1.A = "row2"

	id, err = hood.Save(model1)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if id != 2 {
		t.Fatal("wrong id", id)
	}

	out = nil
	err = hood.Where("a = ? AND j = ?", "string!", 9).Find(&out)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if x := len(out); x != 1 {
		t.Fatal("invalid output length", x)
	}

	out = nil
	err = hood.Where("j = ?", 9).Find(&out)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if x := len(out); x != 2 {
		t.Fatal("invalid output length", x)
	}
}

func TestSqlType(t *testing.T) {
	d := &DialectPg{}
	if x := d.SqlType(true, 0); x != "boolean" {
		t.Fatal("wrong type", x)
	}
	var indirect interface{} = true
	if x := d.SqlType(indirect, 0); x != "boolean" {
		t.Fatal("wrong type", x)
	}
	if x := d.SqlType(uint32(2), 0); x != "integer" {
		t.Fatal("wrong type", x)
	}
	if x := d.SqlType(Id(1), 0); x != "serial" {
		t.Fatal("wrong type", x)
	}
	if x := d.SqlType(int64(1), 0); x != "bigint" {
		t.Fatal("wrong type", x)
	}
	// if x := d.SqlType(Id(1), 0); x != "bigserial" {
	// 	t.Fatal("wrong type", x)
	// }
	if x := d.SqlType(1.8, 0); x != "double precision" {
		t.Fatal("wrong type", x)
	}
	if x := d.SqlType([]byte("asdf"), 0); x != "bytea" {
		t.Fatal("wrong type", x)
	}
	if x := d.SqlType("astring", 0); x != "text" {
		t.Fatal("wrong type", x)
	}
	if x := d.SqlType([]bool{}, 0); x != "varchar(255)" {
		t.Fatal("wrong type", x)
	}
	if x := d.SqlType([]bool{}, 128); x != "varchar(128)" {
		t.Fatal("wrong type", x)
	}
}

func TestCreateTableSql(t *testing.T) {
	hood := New(nil, &DialectPg{})
	type withoutPk struct {
		First  string
		Last   string
		Amount int
	}
	table := &withoutPk{
		"erik",
		"aigner",
		5,
	}
	model, err := interfaceToModel(table, hood.Dialect)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	query := hood.createTableSql(model)
	if query != `CREATE TABLE without_pk ( first text, last text, amount integer )` {
		t.Fatal("wrong query", query)
	}
	type withPk struct {
		Primary Id
		First   string
		Last    string
		Amount  int
	}
	table2 := &withPk{
		First:  "erik",
		Last:   "aigner",
		Amount: 5,
	}
	model, err = interfaceToModel(table2, hood.Dialect)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	query = hood.createTableSql(model)
	if query != `CREATE TABLE with_pk ( primary serial PRIMARY KEY, first text, last text, amount integer )` {
		t.Fatal("wrong query", query)
	}
}

func TestCreateTable(t *testing.T) {
	if disableLiveTests {
		return
	}
	hood := setupDb(t)

	table := &PgDialectModel{}

	hood.DropTable(table)
	err := hood.CreateTable(table)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	err = hood.DropTable(table)
	if err != nil {
		t.Fatal("error not nil", err)
	}
}
