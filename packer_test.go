// Package packer
//
// @author: xwc1125
package packer

import (
	"context"
	"github.com/chain5j/chain5j-pkg/codec/json"
	"github.com/chain5j/chain5j-pkg/event"
	"github.com/chain5j/chain5j-pkg/types"
	"github.com/chain5j/chain5j-pkg/util/convutil"
	"github.com/chain5j/chain5j-pkg/util/dateutil"
	"github.com/chain5j/chain5j-protocol/mock"
	"github.com/chain5j/chain5j-protocol/models"
	"github.com/chain5j/logger"
	"github.com/chain5j/logger/zap"
	"github.com/golang/mock/gomock"
	"math/big"
	"sync"
	"testing"
)

func init() {
	zap.InitWithConfig(&logger.LogConfig{
		Console: logger.ConsoleLogConfig{
			Level:    4,
			Modules:  "*",
			ShowPath: false,
			Format:   "",
			UseColor: true,
			Console:  true,
		},
		File: logger.FileLogConfig{},
	})
}

func TestFactory(t *testing.T) {
	mockCtl := gomock.NewController(nil)
	// config
	mockConfig := mock.NewMockConfig(mockCtl)
	mockConfig.EXPECT().ChainConfig().Return(models.ChainConfig{
		GenesisHeight: 10,
		ChainID:       1,
		ChainName:     "chain5j",
		VersionName:   "v1.0.0",
		VersionCode:   1,
		TxSizeLimit:   128,
		Packer: &models.PackerConfig{
			WorkerType:           models.Timing,
			BlockMaxTxsCapacity:  10000,
			BlockMaxSize:         2048,
			BlockMaxIntervalTime: 1000,
			BlockGasLimit:        5000000,
			Period:               3000,
			EmptyBlocks:          0,
			Timeout:              1000,
			MatchTxsCapacity:     false,
		},
		StateApp: nil,
	}).AnyTimes()
	mockConfig.EXPECT().PackerConfig().Return(models.PackerLocalConfig{
		Metrics:      true,
		MetricsLevel: 2,
	}).AnyTimes()

	// apps
	mockContexts := mock.NewMockAppContexts(mockCtl)
	mockApps := mock.NewMockApps(mockCtl)
	mockApps.EXPECT().NewAppContexts("packer", gomock.Any()).Return(mockContexts, nil)
	mockApps.EXPECT().Prepare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&models.TxsStatus{
		StateRoots: []byte("233456"),
		GasUsed:    21000,
		OkTxs:      getTxs(),
		ErrTxs:     nil,
	})
	mockApps.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// txPool
	mockTxPool := mock.NewMockTxPool(mockCtl)
	//mockTxPool.EXPECT().Subscribe(gomock.Any()).Return(event.NewSubscription(func(quit <-chan struct{}) error {
	//	return nil
	//})).AnyTimes()
	mockTxPool.EXPECT().Delete(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockTxPool.EXPECT().Len().Return(uint64(len(getTxs()))).AnyTimes()
	//mockTxPool.EXPECT().FetchTxs(gomock.Any()).Return(models.NewTransactions(getTxs())).AnyTimes()
	mockTxPool.EXPECT().Delete(gomock.Any(), gomock.Any()).Return().AnyTimes()

	// BlockReadWriter
	mockBlockRW := mock.NewMockBlockReadWriter(mockCtl)
	mockBlockRW.EXPECT().SubscribeChainHeadEvent(gomock.Any()).Return(event.NewSubscription(func(quit <-chan struct{}) error {
		return nil
	})).AnyTimes()
	mockBlockRW.EXPECT().HasBlock(gomock.Any(), gomock.Any()).Return(false).AnyTimes()
	mockBlockRW.EXPECT().InsertBlock(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockBlockRW.EXPECT().CurrentBlock().Return(models.NewBlock(getBlock(), nil, nil)).AnyTimes()
	mockBlockRW.EXPECT().GetHeaderByHash(gomock.Any()).Return(&models.Header{}).AnyTimes()

	// Consensus
	mockEngine := mock.NewMockConsensus(mockCtl)
	//mockEngine.EXPECT().NewChainHead().Return(nil).AnyTimes()
	mockEngine.EXPECT().Prepare(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockEngine.EXPECT().Finalize(gomock.Any(), gomock.Any(), gomock.Any()).Return(models.NewBlock(getBlock(), models.NewTransactions([]models.TransactionSortedList{getTxs()}), nil), nil).AnyTimes()

	mockEngine.EXPECT().Seal(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockerPacker, err := NewPacker(context.Background(),
		WithConfig(mockConfig),
		WithApps(mockApps),
		WithBlockRW(mockBlockRW),
		//WithTxPools(mockTxPools),
		WithEngine(mockEngine),
	)
	if err != nil {
		t.Fatal(err)
	}
	mockerPacker.Start()

	p := mockerPacker.(*packer)
	p.commitNewWork()

	p.resultCh <- models.NewBlock(getBlock(), models.NewTransactions([]models.TransactionSortedList{getTxs()}), nil)
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}

func getBlock() *models.Header {
	return &models.Header{
		ParentHash: types.EmptyHash,
		Height:     10,
		StateRoots: []byte("123456"),
		TxsRoot: []types.Hash{
			types.EmptyHash,
		},
		Timestamp:   uint64(dateutil.CurrentTime()),
		GasUsed:     0,
		GasLimit:    0,
		ArchiveHash: nil,
		Consensus:   nil,
		Extra:       nil,
		Signature:   nil,
	}
}

func getTxs() models.TransactionSortedList {
	models.RegisterTransaction(&MockTransaction{})
	txs := make([]models.Transaction, 0)
	for i := 0; i < 10; i++ {
		tx := MockTransaction{
			Thash:  types.HexToHash(convutil.ToString(i)),
			Tnonce: big.NewInt(int64(i)),
		}
		txs = append(txs, &tx)
	}
	return models.NewTransactionSortedList(txs)
}

var (
	_ models.Transaction = new(MockTransaction)
)

type MockTransaction struct {
	Thash  types.Hash
	Tnonce *big.Int
}

func (m MockTransaction) Less(tx2 models.Transaction) bool {
	//TODO implement me
	panic("implement me")
}

func (m MockTransaction) TxType() types.TxType {
	return types.TxType("TEST")
}

func (m MockTransaction) ChainId() string {
	return "chain5j"
}

func (m MockTransaction) Hash() types.Hash {
	return m.Thash
}

func (m MockTransaction) From() string {
	return "0x9254E62FBCA63769DFd4Cc8e23f630F0785610CE"
}

func (m MockTransaction) To() string {
	return "0x9254E62FBCA63769DFd4Cc8e23f630F0785610CE"
}

func (m MockTransaction) GasLimit() uint64 {
	return 21000
}

func (m MockTransaction) Value() *big.Int {
	return big.NewInt(0)
}

func (m MockTransaction) Input() []byte {
	return []byte("123")
}

func (m MockTransaction) GasPrice() uint64 {
	return 0
}

func (m MockTransaction) Nonce() *big.Int {
	return m.Tnonce
}

func (m MockTransaction) Signer() (types.Address, error) {
	return types.HexToAddress("0x9254E62FBCA63769DFd4Cc8e23f630F0785610CE"), nil
}

func (m MockTransaction) Cost() *big.Int {
	return big.NewInt(0)
}

func (m MockTransaction) Size() types.StorageSize {
	return 0
}

func (m MockTransaction) Serialize() ([]byte, error) {
	return json.Marshal(m)
}

func (m *MockTransaction) Deserialize(d []byte) error {
	var tx MockTransaction
	json.Unmarshal(d, &tx)
	*m = tx
	return nil
}
