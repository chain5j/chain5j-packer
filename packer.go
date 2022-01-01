// Package packer
//
// @author: xwc1125
package packer

import (
	"context"
	"fmt"
	"github.com/chain5j/chain5j-pkg/types"
	"github.com/chain5j/chain5j-pkg/util/dateutil"
	"github.com/chain5j/chain5j-protocol/models"
	"github.com/chain5j/chain5j-protocol/models/eventtype"
	"github.com/chain5j/chain5j-protocol/protocol"
	"github.com/chain5j/logger"
	"go.uber.org/atomic"
	"runtime/debug"
	"sync"
	"time"
)

var (
	_ protocol.Packer = new(packer)
)

const (
	chainHeadChSize   = 64  // 链header 通道size
	transactionChSize = 128 // 交易通道size
)

// packer 打包器
type packer struct {
	log    logger.Logger
	ctx    context.Context
	cancel context.CancelFunc

	config  protocol.Config          // 出块配置
	apps    protocol.Apps            // apps模块
	txPools protocol.TxPools         // 交易池
	blockRW protocol.BlockReadWriter // 数据存储
	engine  protocol.Consensus       // 共识

	running    atomic.Bool // worker是否正在执行【原子状态】
	committing atomic.Bool // 正在打包中的信号

	requestCh chan struct{}      // 打包区块请求
	resultCh  chan *models.Block // 从共识引擎返回的区块

	chainHeadEventCh chan eventtype.ChainHeadEvent // 本地区块
	newTxCh          chan []models.Transaction     // 新交易的ch

	retryCh chan struct{} // 交易触发的情况下，如果出块失败，重新发起请求
	startCh chan struct{} // 开启服务

	envLock sync.Mutex   // env的lock
	env     *environment // 环境变量
}

// environment 环境变量
type environment struct {
	ctx        protocol.AppContexts
	sealCancel context.CancelFunc
}

// getAppCtx 根据交易类型获取context
func (e *environment) getAppCtx(txType types.TxType) protocol.AppContext {
	return e.ctx.Ctx(txType)
}

// NewPacker 创建新的打包器
func NewPacker(rootCtx context.Context, opts ...option) (protocol.Packer, error) {
	ctx, cancel := context.WithCancel(rootCtx)
	p := &packer{
		log:    logger.New("packer"),
		ctx:    ctx,
		cancel: cancel,

		requestCh:        make(chan struct{}, 1),
		resultCh:         make(chan *models.Block, 1),
		chainHeadEventCh: make(chan eventtype.ChainHeadEvent, chainHeadChSize),
		newTxCh:          make(chan []models.Transaction, transactionChSize),
		retryCh:          make(chan struct{}, 1),
		startCh:          make(chan struct{}, 1),
	}
	if err := apply(p, opts...); err != nil {
		p.log.Error("apply is error", "err", err)
		return nil, err
	}
	return p, nil
}

// Start 开启打包服务
func (p *packer) Start() error {
	p.running.Store(true)
	p.committing.Store(false)

	go p.requestLoop() // 启动请求协程
	go p.mainLoop()    // 启动打包协程
	go p.resultLoop()  // 启动结果协程
	p.startCh <- struct{}{}

	return nil
}

// Stop 停止打包服务
func (p *packer) Stop() (err error) {
	defer func() {
		if r := recover(); r != nil {
			p.log.Error("packer stop recover", "err", r)
			debug.PrintStack()
			err = fmt.Errorf("%v", r)
		}
	}()
	p.cancel()
	p.running.Store(false)
	close(p.newTxCh)
	close(p.chainHeadEventCh)
	return nil
}

// requestLoop 区块打包流程的开始, 通过requestCh通知mainLoop.
// 1. 开启服务时触发
// 2. 定时触发
// 3. 交易触发
// 4. 如果还处于共识过程中，则不发送新的request
func (p *packer) requestLoop() {
	timer := time.NewTimer(time.Second)
	<-timer.C

	// 重置定时器
	resetTimer := func() {
		switch p.config.ChainConfig().Packer.WorkerType {
		case models.Timing:
			// 将计时器更改为在持续时间d之后到期
			timer.Reset(p.blockPeriod())
		case models.TransactionTrigger:
			timer.Reset(time.Second)
		case models.BlockStack:
			timer.Reset(p.blockPeriod())
		default:
			// 默认交易触发
			timer.Reset(time.Second)
		}
	}

	// 订阅区块链区块入库的事件
	chainHeadEventSub := p.blockRW.SubscribeChainHeadEvent(p.chainHeadEventCh)
	newTxSub := p.txPools.Subscribe(p.newTxCh)

	for {
		select {
		case <-p.startCh:
			// 启动信号【start时启动的】
			p.newRequest()
			// 发出新请求的信号后，计时器重新计时
			resetTimer()
		case <-p.newTxCh:
			// 有新交易
			p.newRequest()
			resetTimer()
		case <-timer.C:
			// 不为交易触发式出块，或者交易池的交易大于0，启动新的请求
			lenZero := true
			for _, count := range p.txPools.Len() {
				if count > 0 {
					lenZero = false
				}
			}
			if !p.isSealByTx() || !lenZero {
				p.newRequest()
			}
			resetTimer()
		case block := <-p.chainHeadEventCh:
			// 有新的区块产生
			if p.isMetrics(3) {
				p.log.Trace("new db head into", "height", block.Block.Height(), "bHash", block.Block.Hash().Hex())
			}
			// 将已入库的交易从交易池中删除
			if block.Block.Transactions().Len() > 0 {
				for _, list := range block.Block.Transactions() {
					if list.Len() > 0 {
						p.txPools.Delete(list[0].TxType(), list, true)
					}
				}
			}
			if p.engine != nil {
				p.engine.Begin()
			}

			p.cancelCurrent()

			lenZero := true
			for _, count := range p.txPools.Len() {
				if count > 0 {
					lenZero = false
				}
			}
			// 不为交易触发式出块，或者交易池的交易大于0，启动新的请求
			if !p.isSealByTx() || !lenZero {
				p.newRequest()
			}
			resetTimer()
		case err := <-chainHeadEventSub.Err():
			p.log.Error("chainHead event sub err", "err", err)
			return
		case err := <-newTxSub.Err():
			p.log.Error("newTx sub err", "error", err)
			return
		case <-p.ctx.Done():
			return
		}
	}
}

// mainloop 处理区块打包核心逻辑
func (p *packer) mainLoop() {
	for {
		select {
		case <-p.requestCh:
			// 有请求的ch就会触发commit
			p.commit()
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *packer) commit() {
	// 正在打包
	if p.committing.Load() {
		return
	}
	p.committing.Store(true)

	go func() {
		err := p.commitNewWork()
		// 区块到达延迟，重新尝试
		if err == errTimeout {
			err = p.commitNewWork()
		}
		if err != nil {
			//p.retryCh <- struct{}{}
			p.committing.Store(false)
		}
	}()
}

// resultLoop 结果监听的loop
func (p *packer) resultLoop() {
	for {
		select {
		case block := <-p.resultCh:
			// 共识seal处理完成后，通过chan通知数据
			// 如果数据库已经存在，那么无需再做操作
			if block == nil {
				p.log.Error("p-8) result block is nil")
				p.committing.Store(false)
				if p.isMetrics(2) {
					p.log.Debug("p-8) block has exist", "height", block.Height(), "bHash", block.Hash())
				}
				continue
			}
			if p.blockRW.HasBlock(block.Hash(), block.Height()) {
				p.log.Error("p-8) result block is exist", "bHash", block.Hash())
				p.committing.Store(false)
				if p.isMetrics(2) {
					p.log.Debug("p-8) block has exist", "height", block.Height(), "bHash", block.Hash())
				}
				continue
			}

			t2 := time.Now()
			if block.Transactions().Len() > 0 {
				if appCtx := p.currentCtx(); appCtx == nil {
					continue
				} else {
					// app 提交交易
					if err := p.apps.Commit(appCtx, block.Header()); err != nil {
						p.committing.Store(false)
						p.log.Error("p-9) apps commit block err", "err", err)
						p.fallbackTxs(block.Transactions())
						continue
					}
					if p.isMetrics(2) {
						p.log.Debug("p-9) app commit block end", "txsLen", block.Transactions().Len(), "elapsed", dateutil.PrettyDuration(time.Since(t2)))
					}
				}
			}

			// 数据库插入区块
			t2 = time.Now()
			if err := p.blockRW.InsertBlock(block, true); err != nil {
				p.log.Error("p-10) insert block to db error", "height", block.Height(), "bHash", block.Hash().Hex(), "err", err)
				p.fallbackTxs(block.Transactions())
				continue
			}
			if p.isMetrics(2) {
				p.log.Debug("p-10) insert block to db end", "txsLen", block.Transactions().Len(), "height", block.Height(), "bHash", block.Hash().Hex(), "elapsed", dateutil.PrettyDuration(time.Since(t2)))
			}
			t2 = time.Now()
			if block.Transactions().Len() > 0 {
				for _, list := range block.Transactions() {
					if list.Len() > 0 {
						p.txPools.Delete(list[0].TxType(), list, true)
					}
				}
			}
			if p.isMetrics(2) {
				p.log.Debug("p-11) txPool delete txs end", "txsLen", block.Transactions().Len(), "elapsed", dateutil.PrettyDuration(time.Since(t2)))
			}
			p.committing.Store(false)
			p.log.Info("Packer block success", "height", block.Height(), "bHash", block.Hash().Hex(), "txsLen", block.Transactions().Len(), "elapsed", dateutil.GetDistanceTimeToCurrent(int64(block.Header().Timestamp)))
		case <-p.ctx.Done():
			return
		}
	}
}

// commitNewWork 网络提交
func (p *packer) commitNewWork() (err error) {
	var (
		txs models.Transactions
	)
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()
			p.log.Error("commit new work recover", "err", fmt.Errorf("%v", err))
			p.fallbackTxs(txs)
			p.cancelCurrent()
		}
	}()
	t1 := time.Now()
	parent := p.blockRW.CurrentHeader() // 获取当前区块
	height := parent.Height             // 获取当前区块高度
	if p.isMetrics(3) {
		p.log.Debug("p-1) get current header from end", "height", height)
	}
	// 构建header头
	t2 := time.Now()
	header := &models.Header{
		ParentHash: parent.Hash(),
		Height:     height + 1,
		StateRoots: parent.StateRoots,
		Timestamp:  uint64(dateutil.CurrentTime()),
		GasUsed:    0,
		GasLimit:   p.config.ChainConfig().Packer.BlockGasLimit,
		Extra:      []byte{},
	}
	// 更新环境
	if err := p.updateEnvironment(parent.StateRoots); err != nil {
		p.log.Error("p-2) update environment err", "err", err)
		return err
	}
	if p.isMetrics(3) {
		p.log.Debug("p-2) new header end", "Height", header.Height, "elapsed", dateutil.PrettyDuration(time.Since(t2)))
	}

	// 共识预处理
	if p.engine != nil {
		t2 := time.Now()
		if err = p.engine.Prepare(p.blockRW, header); err != nil {
			p.log.Error("p-3) engine failed to prepare header", "err", err)
			return err
		}
		if p.isMetrics(2) {
			p.log.Debug("p-3) engine prepare header end", "elapsed", dateutil.PrettyDuration(time.Since(t2)))
		}
	}

	// 从交易池获取交易
	t2 = time.Now()
	txs = p.txPools.FetchTxs(p.config.ChainConfig().Packer.BlockMaxTxsCapacity, header.Timestamp)
	if txs != nil && txs.Len() > 0 {
		if p.config.ChainConfig().Packer.MatchTxsCapacity &&
			uint64(txs.Len()) < p.config.ChainConfig().Packer.BlockMaxTxsCapacity {
			p.fallbackTxs(txs)
			return errWaitTransaction
		}
		if p.isMetrics(1) {
			p.log.Info("p-4) txPool fetchTxs end", "txsLen", txs.Print(), "elapsed", dateutil.PrettyDuration(time.Since(t2)))
		}
	} else {
		if p.isSealByTx() {
			if p.isMetrics(3) {
				p.log.Debug(errWaitTransaction.Error())
			}
			return errWaitTransaction
		}
	}

	var okTxs models.Transactions
	{
		if txs != nil && txs.Len() > 0 {
			t2 = time.Now()
			okTxs, header.GasUsed, header.StateRoots, err = p.commitTransactions(header, txs, header.GasLimit)
			if err != nil {
				p.log.Error("p-5) commit transactions error", "error", err)
				p.fallbackTxs(txs)
				return err
			}
			if p.isMetrics(2) {
				p.log.Debug("p-5) commit txs end", "txsLen", txs.Len(), "elapsed", dateutil.PrettyDuration(time.Since(t2)))
			}
		}
	}

	// 共识处理区块
	t2 = time.Now()
	block, err := p.engine.Finalize(p.blockRW, header, models.NewTransactions(okTxs))
	if err != nil {
		p.log.Error("p-6) engine finalize header err", "height", header.Height, "err", err)
		p.fallbackTxs(txs)
		return err
	}
	if p.isMetrics(1) {
		p.log.Info("p-6) engine finalize header end", "height", block.Height(), "txsLen", block.Transactions().Len(), "elapsed", dateutil.PrettyDuration(time.Since(t1)))
	}
	// 等待共识seal
	t2 = time.Now()
	ctx, cancelFunc := context.WithTimeout(p.ctx, time.Duration(p.config.ChainConfig().Packer.Timeout)*time.Second) // todo 需要改成毫秒
	p.setEnvironmentCancel(cancelFunc)
	if err = p.engine.Seal(ctx, p.blockRW, block, p.resultCh); err != nil {
		p.log.Error("p-7) engine seal block err", "err", err, "elapsed", dateutil.PrettyDuration(time.Since(t2)))
		p.fallbackTxs(block.Transactions())
		return err
	}
	if p.isMetrics(1) {
		p.log.Debug("p-7) engine seal block end", "elapsed", dateutil.PrettyDuration(time.Since(t2)))
	}

	p.log.Info("Commit new block end", "height", block.Height(), "txsLen", block.Transactions().Len(), "elapsed", dateutil.PrettyDuration(time.Since(t1)))
	return nil
}

// fallbackTxs 回滚交易
func (p *packer) fallbackTxs(txs models.Transactions) error {
	var err error
	if txs != nil && txs.Len() > 0 {
		for _, txList := range txs {
			if txList.Len() > 0 {
				txType := txList[0].TxType()
				if err1 := p.txPools.Fallback(txType, txList); err1 != nil {
					err = fmt.Errorf("%v,txType fallback err:%v", err, err1)
				}
			}
		}
	}
	return err
}

// commitTransactions 提交交易
func (p *packer) commitTransactions(header *models.Header, txs models.Transactions, gasLimit uint64) (okTxs models.Transactions, gasUsed uint64, root []byte, err error) {
	t1 := time.Now()
	var parentRoot []byte
	if parent := p.blockRW.GetHeaderByHash(header.ParentHash); parent == nil {
		p.log.Error("p-5_1) get header by hash err", "hash", header.ParentHash.Hex())
		return nil, 0, nil, errGetHeader
	} else {
		parentRoot = parent.StateRoots
	}
	if p.isMetrics(2) {
		p.log.Debug("p-5_1) get header by hash end", "elapsed", dateutil.PrettyDuration(time.Since(t1)))
	}

	appCtx := p.currentCtx()
	if appCtx == nil {
		return nil, 0, nil, errAppContext
	}
	// 预处理交易
	t2 := time.Now()
	stateRoots, txsStatus := p.apps.Prepare(appCtx, parentRoot, header, txs, gasLimit)
	if txsStatus == nil {
		return nil, 0, nil, errAppPrepareTxs
	}
	if p.isMetrics(2) {
		p.log.Debug("p-5_2) app prepare txs end", "txsLen", txs.Len(), "elapsed", dateutil.PrettyDuration(time.Since(t2)))
	}

	if txsStatus != nil {
		okTxs1 := make([]models.TransactionSortedList, 0, len(txsStatus))
		for _, txStatus := range txsStatus {
			if txStatus.ErrTxs.Len() > 0 {
				t2 = time.Now()
				p.txPools.Delete(txStatus.TxType, txStatus.ErrTxs, false)
				if p.isMetrics(2) {
					p.log.Debug("p-5_3) txPool delete txs end", "txType", txStatus.TxType, "errTxsLen", txStatus.ErrTxs.Len(), "elapsed", dateutil.PrettyDuration(time.Since(t2)))
				}
			}
			okTxs1 = append(okTxs1, txStatus.OkTxs)
			gasUsed = gasUsed + txStatus.GasUsed
		}
		return okTxs1, gasUsed, stateRoots, nil
	}

	return okTxs, gasUsed, stateRoots, nil
}

// isSealByTx 是否交易触发
func (p *packer) isSealByTx() bool {
	blockConfig := p.config.ChainConfig().Packer
	isSealByTx := p.config.ChainConfig().Packer.BlockMaxIntervalTime == 0 // =0时，采用交易触发
	if isSealByTx {
		// 交易触发
		return true
	}
	if blockConfig.EmptyBlocks == 0 {
		return false
	}
	return false
}

// blockPeriod 区块产生间隔
func (p *packer) blockPeriod() time.Duration {
	if p.config.ChainConfig().Packer.Period == 0 {
		return time.Second
	}
	return time.Duration(p.config.ChainConfig().Packer.Period) * time.Millisecond
}

// isRunning 判断是否正在打包
func (p *packer) isRunning() bool {
	return p.running.Load()
}

// newRequest 检测新的请求
func (p *packer) newRequest() {
	if !p.isRunning() {
		return
	}
	if len(p.requestCh) == 0 {
		// 没有打包区块请求，并且没有committing信号
		if !p.committing.Load() {
			p.requestCh <- struct{}{}
		}
	} else {
		if p.isMetrics(3) {
			p.log.Trace("already send seal block request, discard")
		}
	}
}

// updateEnvironment 更新环境
func (p *packer) updateEnvironment(root []byte) error {
	p.envLock.Lock()
	defer p.envLock.Unlock()

	ctx, err := p.apps.NewAppContexts("packer", root)
	if err != nil {
		return err
	}

	p.env = &environment{
		ctx: ctx,
	}

	return nil
}

// setEnvironmentCancel 设置环境
func (p *packer) setEnvironmentCancel(cancelFunc context.CancelFunc) {
	p.envLock.Lock()
	defer p.envLock.Unlock()
	if p.env == nil {
		return
	}

	p.env.sealCancel = cancelFunc
}

// currentCtx 当前的ctx
func (p *packer) currentCtx() protocol.AppContexts {
	p.envLock.Lock()
	defer p.envLock.Unlock()
	if p.env == nil {
		return nil
	}

	return p.env.ctx
}

// cancelCurrent 取消当前的seal，并将committing设置为0
func (p *packer) cancelCurrent() {
	p.envLock.Lock()
	defer p.envLock.Unlock()

	if p.env == nil {
		return
	}

	if p.env.sealCancel != nil {
		p.env.sealCancel()
	}
	p.committing.Store(false)
}

func (p *packer) isMetrics(_metricsLevel uint64) bool {
	return p.config.PackerConfig().IsMetrics(_metricsLevel)
}
