// The MIT License (MIT)
//
// # Copyright (c) 2016 xtaci
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package std

import (
	"encoding/csv"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

// snmpWriter manages a CSV writer for SNMP logging with header tracking.
type snmpWriter struct {
	w          *csv.Writer
	record     []string
	headerCols []string
	written    bool
}

// newSnmpWriter creates a snmpWriter bound to f.
// If f already has content (reopen within the same rotation window), the
// header is treated as already written so a second header is not appended.
func newSnmpWriter(f *os.File) *snmpWriter {
	cols := kcp.DefaultSnmp.Header()
	rec := make([]string, 1+len(cols))
	sw := &snmpWriter{
		w:          csv.NewWriter(f),
		record:     rec,
		headerCols: cols,
	}
	if info, err := f.Stat(); err == nil && info.Size() > 0 {
		sw.written = true
	}
	return sw
}

// writeTick writes one SNMP data row, preceded by the header row on first call.
func (sw *snmpWriter) writeTick() error {
	if !sw.written {
		sw.record[0] = "Unix"
		copy(sw.record[1:], sw.headerCols)
		if err := sw.w.Write(sw.record); err != nil {
			return err
		}
		sw.written = true
	}
	sw.record[0] = strconv.FormatInt(time.Now().Unix(), 10)
	copy(sw.record[1:], kcp.DefaultSnmp.ToSlice())
	if err := sw.w.Write(sw.record); err != nil {
		return err
	}
	sw.w.Flush()
	return sw.w.Error()
}

func SnmpLogger(path string, interval int) {
	if path == "" || interval <= 0 {
		return
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	logdir, logfile := filepath.Split(path)
	var f *os.File
	var currentPath string
	var sw *snmpWriter

	for range ticker.C {
		formattedPath := logdir + time.Now().Format(logfile)
		if formattedPath != currentPath {
			if f != nil {
				f.Close()
			}
			var err error
			f, err = os.OpenFile(formattedPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
			if err != nil {
				log.Println("snmp logger:", err)
				f = nil
				currentPath = ""
				sw = nil
				continue
			}
			currentPath = formattedPath
			sw = newSnmpWriter(f)
		}
		if f == nil || sw == nil {
			continue
		}
		if err := sw.writeTick(); err != nil {
			log.Println("snmp logger:", err)
		}
	}
}
