package std

import (
	"encoding/csv"
	"os"
	"strconv"
	"testing"

	kcp "github.com/xtaci/kcp-go/v5"
)

func readAllCSV(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("csv read %s: %v", path, err)
	}
	return rows
}

func TestSnmpWriter_NewFile_WritesHeaderAndData(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/snmp.csv"
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	sw := newSnmpWriter(f)
	if err := sw.writeTick(); err != nil {
		t.Fatalf("writeTick: %v", err)
	}
	f.Sync() // flush to disk before reading
	f.Close()

	rows := readAllCSV(t, path)
	if len(rows) < 2 {
		t.Fatalf("got %d rows, want >= 2", len(rows))
	}

	// First row is the header: "Unix" + DefaultSnmp.Header() columns.
	hdr := append([]string{"Unix"}, kcp.DefaultSnmp.Header()...)
	for i, want := range hdr {
		if i >= len(rows[0]) {
			t.Fatalf("header row short: missing column %d (%q)", i, want)
		}
		if rows[0][i] != want {
			t.Errorf("header col %d: got %q, want %q", i, rows[0][i], want)
		}
	}

	// Second row is a data row. First field should be a parseable timestamp.
	if _, err := strconv.ParseInt(rows[1][0], 10, 64); err != nil {
		t.Errorf("data row col 0: not a unix timestamp: %v", err)
	}

	// Column count: data row = 1 + header count.
	wantCols := 1 + len(kcp.DefaultSnmp.Header())
	if len(rows[1]) != wantCols {
		t.Errorf("data row columns: got %d, want %d", len(rows[1]), wantCols)
	}
}

func TestSnmpWriter_SubsequentTicks_NoDuplicateHeader(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/snmp.csv"
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	sw := newSnmpWriter(f)

	// Tick twice.
	if err := sw.writeTick(); err != nil {
		t.Fatalf("writeTick #1: %v", err)
	}
	if err := sw.writeTick(); err != nil {
		t.Fatalf("writeTick #2: %v", err)
	}
	f.Sync()
	f.Close()

	rows := readAllCSV(t, path)
	// Expect 1 header + 2 data rows = 3 rows.
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3 (1 header + 2 data)", len(rows))
	}

	// Row 0 is header; rows 1 and 2 are data rows with timestamps.
	for i := 0; i < 3; i++ {
		firstCol := rows[i][0]
		if i == 0 {
			if firstCol != "Unix" {
				t.Errorf("row 0 col 0: got %q, want Unix", firstCol)
			}
		} else {
			if _, err := strconv.ParseInt(firstCol, 10, 64); err != nil {
				t.Errorf("row %d col 0: not a unix timestamp: %v", i, err)
			}
		}
	}
}

func TestSnmpWriter_DataRow_HasTimestamp(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/snmp.csv"
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	sw := newSnmpWriter(f)
	if err := sw.writeTick(); err != nil {
		t.Fatalf("writeTick: %v", err)
	}
	f.Sync()
	f.Close()

	rows := readAllCSV(t, path)
	if len(rows) < 2 {
		t.Fatalf("want >= 2 rows, got %d", len(rows))
	}
	ts, err := strconv.ParseInt(rows[1][0], 10, 64)
	if err != nil {
		t.Fatalf("data row timestamp not int64: %v", err)
	}
	if ts <= 0 {
		t.Errorf("timestamp should be positive, got %d", ts)
	}
}

func TestSnmpWriter_ColumnCount(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/snmp.csv"
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	sw := newSnmpWriter(f)
	if err := sw.writeTick(); err != nil {
		t.Fatalf("writeTick: %v", err)
	}
	f.Sync()
	f.Close()

	rows := readAllCSV(t, path)
	expectedCols := 1 + len(kcp.DefaultSnmp.Header())
	if len(rows[0]) != expectedCols {
		t.Errorf("header cols: got %d, want %d", len(rows[0]), expectedCols)
	}
	if len(rows[1]) != expectedCols {
		t.Errorf("data cols: got %d, want %d", len(rows[1]), expectedCols)
	}
}

func TestSnmpWriter_Rotation_NewHeader(t *testing.T) {
	dir := t.TempDir()

	// First file: tick once, verify header written.
	path1 := dir + "/snmp1.csv"
	f1, err := os.Create(path1)
	if err != nil {
		t.Fatalf("create f1: %v", err)
	}
	sw1 := newSnmpWriter(f1)
	if err := sw1.writeTick(); err != nil {
		t.Fatalf("writeTick f1: %v", err)
	}
	f1.Sync()
	f1.Close()

	rows1 := readAllCSV(t, path1)
	if len(rows1) < 2 || rows1[0][0] != "Unix" {
		t.Fatalf("first file missing header")
	}

	// Second file (simulating rotation): a fresh snmpWriter.
	path2 := dir + "/snmp2.csv"
	f2, err := os.Create(path2)
	if err != nil {
		t.Fatalf("create f2: %v", err)
	}
	sw2 := newSnmpWriter(f2)
	if err := sw2.writeTick(); err != nil {
		t.Fatalf("writeTick f2: %v", err)
	}
	f2.Sync()
	f2.Close()

	rows2 := readAllCSV(t, path2)
	if len(rows2) < 2 || rows2[0][0] != "Unix" {
		t.Fatalf("rotated file missing header")
	}
}

func TestSnmpWriter_ReopenNonEmpty_NoDuplicateHeader(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/snmp.csv"

	f1, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	sw1 := newSnmpWriter(f1)
	if err := sw1.writeTick(); err != nil {
		t.Fatalf("writeTick 1: %v", err)
	}
	f1.Sync()
	f1.Close()

	// Reopen the same non-empty file (same rotation window / process restart).
	f2, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	sw2 := newSnmpWriter(f2)
	if err := sw2.writeTick(); err != nil {
		t.Fatalf("writeTick 2: %v", err)
	}
	f2.Sync()
	f2.Close()

	rows := readAllCSV(t, path)
	headers := 0
	for _, row := range rows {
		if len(row) > 0 && row[0] == "Unix" {
			headers++
		}
	}
	if headers != 1 {
		t.Fatalf("header rows=%d, want 1 (reopen must not rewrite CSV header)", headers)
	}
	if len(rows) != 3 { // 1 header + 2 data
		t.Fatalf("got %d rows, want 3 (1 header + 2 data)", len(rows))
	}
}

func BenchmarkWriteSnmpTick(b *testing.B) {
	b.ReportAllocs()

	dir := b.TempDir()
	path := dir + "/snmp.csv"
	f, err := os.Create(path)
	if err != nil {
		b.Fatalf("create: %v", err)
	}
	defer f.Close()

	sw := newSnmpWriter(f)
	// Write header only once (outside the benchmark loop) to match
	// steady-state per-tick cost.
	if err := sw.writeTick(); err != nil {
		b.Fatalf("initial writeTick: %v", err)
	}

	b.ResetTimer()
	for b.Loop() {
		if err := sw.writeTick(); err != nil {
			b.Fatalf("writeTick: %v", err)
		}
	}
}
