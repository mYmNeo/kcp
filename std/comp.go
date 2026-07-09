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
	"net"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
)

// CompStream is a net.Conn wrapper that transparently compresses data using LZ4.
// LZ4's internal 4MB buffer is pooled by the lz4 library itself, so no
// additional pooling is needed at this layer.
type CompStream struct {
	conn net.Conn
	w    *lz4.Writer
	r    *lz4.Reader
}

func (c *CompStream) Read(p []byte) (n int, err error) {
	return c.r.Read(p)
}

func (c *CompStream) Write(p []byte) (n int, err error) {
	n, err = c.w.Write(p)
	// Flush small writes to keep interactive traffic responsive (e.g. SSH keystrokes).
	// Large writes (bulk io.Copy with 32KB buffer) batch into full LZ4 blocks
	// for better compression ratio.
	if len(p) < 1024 {
		if flushErr := c.w.Flush(); flushErr != nil && err == nil {
			err = flushErr
		}
	}
	return n, errors.WithStack(err)
}

func (c *CompStream) Close() error {
	err := c.w.Close()
	if closeErr := c.conn.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (c *CompStream) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *CompStream) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *CompStream) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *CompStream) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *CompStream) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// NewCompStream creates a new stream that transparently compresses data using LZ4.
func NewCompStream(conn net.Conn) *CompStream {
	return &CompStream{
		conn: conn,
		w:    lz4.NewWriter(conn),
		r:    lz4.NewReader(conn),
	}
}
