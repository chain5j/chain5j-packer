// Package packer
//
// @author: xwc1125
package packer

import (
	"fmt"
	"github.com/chain5j/chain5j-protocol/protocol"
)

type option func(f *packer) error

func apply(f *packer, opts ...option) error {
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(f); err != nil {
			return fmt.Errorf("option apply err:%v", err)
		}
	}
	return nil
}

func WithApps(apps protocol.Apps) option {
	return func(f *packer) error {
		f.apps = apps
		return nil
	}
}

func WithConfig(config protocol.Config) option {
	return func(f *packer) error {
		f.config = config
		return nil
	}
}

func WithBlockRW(blockRW protocol.BlockReadWriter) option {
	return func(f *packer) error {
		f.blockRW = blockRW
		return nil
	}
}

func WithEngine(engine protocol.Consensus) option {
	return func(f *packer) error {
		f.engine = engine
		return nil
	}
}

func WithTxPools(txPools protocol.TxPools) option {
	return func(f *packer) error {
		f.txPools = txPools
		return nil
	}
}
