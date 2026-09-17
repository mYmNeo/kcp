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

package main

import (
	"github.com/xtaci/kcptun/std"
	"github.com/xtaci/smux"
)

// Config models the client-side configuration loaded via flags or JSON.
type Config struct {
	std.BaseConfig        // Embed shared configuration
	LocalAddr      string `json:"localaddr"`
	RemoteAddr     string `json:"remoteaddr"`
	Conn           int    `json:"conn"`
	TCP            bool   `json:"tcp"`

	// CarrierStreams is the number of parallel TCP streams this client opens per
	// KCP connection when the carrier transport is selected. It is dialer-side
	// only: the listener adapts to however many streams arrive.
	CarrierStreams int `json:"carrierstreams"`

	// CarrierSecret authenticates the carrier handshake. It is the same
	// PBKDF2-derived key the block crypt uses, so it is never read from a config
	// file and never logged.
	CarrierSecret []byte       `json:"-"`
	AutoExpire    int          `json:"autoexpire"`
	ScavengeTTL   int          `json:"scavengettl"`
	UseConntrack  bool         `json:"conntrack"`
	ShmMap        string       `json:"shmmap"`
	SmuxConfig    *smux.Config `json:"-"` // precomputed smux configuration
}

func parseJSONConfig(config *Config, path string) error {
	return std.ParseJSONConfig(config, path)
}
