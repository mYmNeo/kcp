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

func SnmpLogger(path string, interval int) {
	if path == "" || interval <= 0 {
		return
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	logdir, logfile := filepath.Split(path)
	var f *os.File
	var currentPath string

	for range ticker.C {
		formattedPath := logdir + time.Now().Format(logfile)
		if formattedPath != currentPath {
			if f != nil {
				f.Close()
			}
			var err error
			f, err = os.OpenFile(formattedPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o666)
			if err != nil {
				log.Println("snmp logger:", err)
				f = nil
				currentPath = ""
				continue
			}
			currentPath = formattedPath
		}
		if f == nil {
			continue
		}
		if err := writeSnmpRecord(f); err != nil {
			log.Println("snmp logger:", err)
		}
	}
}

// writeSnmpRecord writes a single SNMP record to f.
func writeSnmpRecord(f *os.File) error {
	// Check if the file is empty to write the header.
	if stat, err := f.Stat(); err == nil && stat.Size() == 0 {
		w := csv.NewWriter(f)
		if err := w.Write(append([]string{"Unix"}, kcp.DefaultSnmp.Header()...)); err != nil {
			return err
		}
		w.Flush()
		if err := w.Error(); err != nil {
			return err
		}
	}

	w := csv.NewWriter(f)
	if err := w.Write(append([]string{strconv.FormatInt(time.Now().Unix(), 10)}, kcp.DefaultSnmp.ToSlice()...)); err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}
