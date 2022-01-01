// Package packer
//
// @author: xwc1125
package packer

import "errors"

var (
	errTxPoolEmpty     = errors.New("txPool txs is empty")
	errWaitTransaction = errors.New("waiting for transactions")
	errTimeout         = errors.New("worker timeout")
	errGetHeader       = errors.New("get header err")
	errAppContext      = errors.New("app context is nil")
	errAppPrepareTxs   = errors.New("app prepare txs err")
)
