package arsyncer

import (
	"fmt"
	"github.com/inconshreveable/log15"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duke-git/lancet/v2/slice"
	"github.com/everFinance/goar"
	"github.com/everFinance/goar/types"
	"github.com/everFinance/goar/utils"
	"github.com/go-co-op/gocron"
	"github.com/panjf2000/ants/v2"
)

var log log15.Logger

const (
	SubscribeTypeTx         = "subscribe_tx"
	SubscribeTypeBlock      = "subscribe_block"
	SubscribeTypeBlockAndTx = "subscribe_block_and_tx"
)

type Syncer struct {
	curHeight int64
	FilterParams
	blockChan            chan *types.Block
	blockTxsChan         chan []SubscribeTx
	SubscribeChan        chan []SubscribeTx
	arClient             *goar.Client
	nextSubscribeTxBlock int64
	conNum               int64 // concurrency of number
	stableDistance       int64 // stable block distance
	blockIdxs            *BlockIdxs
	scheduler            *gocron.Scheduler
	peers                []string
	importantPeers       []string

	subscribeType      string
	SubscribeBlockChan chan *types.Block
	isCloseBlock       atomic.Int32
	isCloseTx          atomic.Int32
}

func New(startHeight int64, filterParams FilterParams, arNode string, conNum int, stableDistance int64, subscribeType string, logPath string, impPeers []string) *Syncer {
	log = NewLog("syncer", logPath)
	if conNum <= 0 {
		conNum = 10 // default concurrency of number is 10
	}
	if stableDistance <= 0 {
		stableDistance = 15 // suggest stable block distance is 15
	}
	arCli := goar.NewClient(arNode)

	fmt.Println("Init arweave block indep hash_list, need to speed about 2 minutes...")
	idxs, err := GetBlockIdxs(startHeight, arCli)
	if err != nil {
		panic(err)
	}
	fmt.Println("Init arweave block indep hash_list finished...")
	fmt.Printf("start=%v||end=%v||len=%v\n", idxs.StartHeight, idxs.EndHeight, len(idxs.IndepHashMap))

	peers, err := arCli.GetPeers()
	if err != nil {
		panic(err)
	}
	if impPeers != nil && len(impPeers) > 0 {
		peers = slice.Without(peers)
		peers = append(impPeers, peers...)
	}
	return &Syncer{
		curHeight:            startHeight,
		FilterParams:         filterParams,
		blockChan:            make(chan *types.Block, 5*conNum),
		SubscribeBlockChan:   make(chan *types.Block, 5*conNum),
		blockTxsChan:         make(chan []SubscribeTx, conNum),
		SubscribeChan:        make(chan []SubscribeTx, conNum),
		arClient:             arCli,
		nextSubscribeTxBlock: startHeight,
		conNum:               int64(conNum),
		stableDistance:       stableDistance,
		blockIdxs:            idxs,
		scheduler:            gocron.NewScheduler(time.UTC),
		peers:                peers,
		importantPeers:       impPeers,
		subscribeType:        subscribeType,
	}
}

func (s *Syncer) Run() {
	go s.runJobs()
	go s.pollingBlock()
	go s.pollingTx()
	go s.filterTx()
}

func (s *Syncer) CloseBlockCh() {
	if s.isCloseBlock.CompareAndSwap(0, 1) {
		close(s.blockChan)
		close(s.SubscribeBlockChan)
		s.clearJobs()
	}
}
func (s *Syncer) CloseTxCh() {
	if s.isCloseBlock.CompareAndSwap(0, 1) {
		close(s.blockChan)
		close(s.SubscribeBlockChan)
		s.clearJobs()
	}
}
func (s *Syncer) Close() (subscribeHeight int64) {
	close(s.blockChan)
	close(s.blockTxsChan)
	close(s.SubscribeChan)
	close(s.SubscribeBlockChan)
	return s.nextSubscribeTxBlock - 1
}

func (s *Syncer) SubscribeTxCh() <-chan []SubscribeTx {
	return s.SubscribeChan
}

func (s *Syncer) SubscribeBlockCh() <-chan *types.Block {
	return s.SubscribeBlockChan
}

func (s *Syncer) GetSyncedHeight() int64 {
	return atomic.LoadInt64(&s.nextSubscribeTxBlock)
}

func (s *Syncer) pollingBlock() {
	for {
		info, err := s.arClient.GetInfo()
		if err != nil {
			log.Error("get info", "err", err)
			time.Sleep(10 * time.Second)
			continue
		}
		stableHeight := info.Height - s.stableDistance
		log.Debug("stable block", "height", stableHeight)

		if s.curHeight >= stableHeight {
			log.Debug("synced curHeight must less than on chain stableHeight; please wait 2 minute", "curHeight", s.curHeight, "stableHeight", stableHeight)
			time.Sleep(2 * time.Minute)
			continue
		}

		for s.curHeight <= stableHeight {
			start := s.curHeight
			end := start + s.conNum
			if end > stableHeight {
				end = stableHeight
			}
			blocks := mustGetBlocks(start, end, s.arClient, s.blockIdxs, int(s.conNum), s.peers)
			log.Info("get blocks success", "start", start, "end", end, "curHeight", s.curHeight, "stableHeight", stableHeight)

			s.curHeight = end + 1
			// add chan
			for _, b := range blocks {
				if s.subscribeType == SubscribeTypeTx || s.subscribeType == SubscribeTypeBlockAndTx {
					s.blockChan <- b
				}
				if s.subscribeType == SubscribeTypeBlock || s.subscribeType == SubscribeTypeBlockAndTx {
					s.SubscribeBlockChan <- b
				}
			}
		}
	}
}

func (s *Syncer) pollingTx() {
	for {
		select {
		case b, ok := <-s.blockChan:
			if !ok {
				return
			}
			bHeight := b.Height
			for {
				if bHeight-atomic.LoadInt64(&s.nextSubscribeTxBlock) < s.conNum {
					break
				}
				// log.Debug("wait for pollingTxs", "wait block height", b.Height, "nextSubscribeTxBlock Height", s.nextSubscribeTxBlock)
				time.Sleep(5 * time.Second)
			}

			go s.getTxs(*b)
		}
	}
}

func (s *Syncer) getTxs(b types.Block) {
	txs := mustGetTxs(b.Height, b.Txs, s.arClient, int(s.conNum), s.peers)

	// subscribe txs
	for {
		if b.Height == atomic.LoadInt64(&s.nextSubscribeTxBlock) {
			txsChan := make([]SubscribeTx, 0, len(txs))
			for _, tx := range txs {
				sTx := SubscribeTx{
					Transaction:    tx,
					BlockHeight:    b.Height,
					BlockId:        b.IndepHash,
					BlockTimestamp: b.Timestamp,
				}
				txsChan = append(txsChan, sTx)
			}
			if len(txsChan) > 0 {
				s.blockTxsChan <- txsChan
			}
			log.Info("polling one block txs success", "height", b.Height, "txNum", len(txs))
			atomic.AddInt64(&s.nextSubscribeTxBlock, 1)
			break
		}
		time.Sleep(2 * time.Second)
	}
}

func (s *Syncer) filterTx() {
	for {
		select {
		case txs, ok := <-s.blockTxsChan:
			if !ok {
				return
			}
			filterTxs := make([]SubscribeTx, 0, len(txs))
			for _, tx := range txs {
				if filter(s.FilterParams, tx.Transaction) {
					continue
				}
				filterTxs = append(filterTxs, tx)
			}

			if len(filterTxs) > 0 {
				s.SubscribeChan <- filterTxs
			}
		}
	}
}

func mustGetTxs(blockHeight int64, blockTxs []string, arClient *goar.Client, conNum int, peers []string) (txs []types.Transaction) {
	if len(blockTxs) == 0 {
		return
	}

	var (
		lock sync.Mutex
		wg   sync.WaitGroup
	)

	// for sort
	txIdxMap := make(map[string]int)
	for idx, txId := range blockTxs {
		txIdxMap[txId] = idx
	}
	txs = make([]types.Transaction, len(blockTxs))

	p, _ := ants.NewPoolWithFunc(conNum, func(i interface{}) {
		txId := i.(string)
		tx, err := getTxByIdRetry(blockHeight, arClient, txId, peers)
		if err != nil {
			log.Error("get tx by id error", "txId", txId, "err", err)
			// notice: must return fetch failed tx
			tx = types.Transaction{ID: txId}
		}

		lock.Lock()
		idx := txIdxMap[tx.ID]
		txs[idx] = tx
		lock.Unlock()

		wg.Done()
	})

	defer p.Release()

	for _, txId := range blockTxs {
		wg.Add(1)
		_ = p.Invoke(txId)
	}
	wg.Wait()

	return
}

func mustGetBlocks(start, end int64, arClient *goar.Client, blockIdxs *BlockIdxs, conNum int, peers []string) (blocks []*types.Block) {
	if start > end {
		return
	}

	blocks = make([]*types.Block, end-start+1)
	var (
		lock sync.Mutex
		wg   sync.WaitGroup
	)

	p, _ := ants.NewPoolWithFunc(conNum, func(i interface{}) {
		height := i.(int64)
		b, err := getBlockByHeightRetry(arClient, height, blockIdxs, peers)
		if err != nil {
			log.Error("getBlockByHeightRetry get block by height error", "height", height, "err", err)
			panic(err)
		}

		lock.Lock()
		blocks[height-start] = b
		lock.Unlock()

		wg.Done()
	}, ants.WithPanicHandler(func(err interface{}) {
		panic(err)
	}))

	defer p.Release()

	for i := start; i <= end; i++ {
		wg.Add(1)
		_ = p.Invoke(i)
	}
	wg.Wait()

	return
}

func getTxByIdRetry(blockHeight int64, arCli *goar.Client, txId string, peers []string) (types.Transaction, error) {
	count := 0
	for {
		// get from trust node
		tx, err := arCli.GetTransactionByID(txId)
		if err == nil {
			// verify tx, ignore genesis block txs
			if blockHeight != 0 {
				err = utils.VerifyTransaction(*tx)
			}
		}

		if err != nil {
			// get from non-trust nodes
			tx, err = arCli.GetTxFromPeers(txId, peers...)
			if err == nil {
				// verify tx, ignore genesis block txs
				if blockHeight != 0 {
					err = utils.VerifyTransaction(*tx)
				}
			}
		}

		if err == nil {
			return *tx, nil
		}

		if count == 5 {
			return types.Transaction{}, err
		}
		count++
		time.Sleep(2 * time.Second)
	}
}

func getTxAndDataByIdRetry(blockHeight int64, arCli *goar.Client, txId string, peers []string) (types.Transaction, error) {
	count := 0
	for {
		// get from trust node
		tx, err := arCli.GetTransactionByID(txId)
		if err == nil {
			// get data
			body, dataErr := arCli.GetTransactionData(txId)
			if dataErr != nil {
				err = dataErr
			} else {
				tx.Data = string(body)
			}
		}
		if err == nil {
			// verify tx, ignore genesis block txs
			if blockHeight != 0 {
				err = utils.VerifyTransaction(*tx)
			}
		}

		if err != nil {
			// get from non-trust nodes
			tx, err = arCli.GetTxFromPeers(txId, peers...)
			if err == nil {
				// get data
				body, dataErr := arCli.GetTxDataFromPeers(txId, peers...)
				if dataErr != nil {
					err = dataErr
				} else {
					tx.Data = string(body)
				}

			}
			if err == nil {
				// verify tx, ignore genesis block txs
				if blockHeight != 0 {
					err = utils.VerifyTransaction(*tx)
				}
			}
		}

		if err == nil {
			return *tx, nil
		}

		if count == 5 {
			return types.Transaction{}, err
		}
		count++
		time.Sleep(2 * time.Second)
	}
}
func getBlockByHeightRetry(arCli *goar.Client, height int64, blockIdxs *BlockIdxs, peers []string) (*types.Block, error) {
	count := 0
	for {
		b, err := arCli.GetBlockByHeight(height)
		if err == nil {
			// verify block
			err = blockIdxs.VerifyBlock(*b)
		}
		if err != nil {
			b, err = arCli.GetBlockFromPeers(height, peers...)
			if err == nil {
				// verify block
				err = blockIdxs.VerifyBlock(*b)
			}
		}

		if err == nil {
			return b, nil
		}

		if count == 5 {
			return nil, err
		}
		count++
		time.Sleep(2 * time.Second)
	}
}

func filter(params FilterParams, tx types.Transaction) bool {
	if tx.Owner == "" { // todo this tx is incorrect, need subscribe to coder debug
		return false
	}

	if params.OwnerAddress != "" {
		// filer owner address
		addr, err := utils.OwnerToAddress(tx.Owner)
		if err != nil {
			log.Error("utils.OwnerToAddress(tx.Owner) err", "err", err, "owner", tx.Owner)
			return true
		}
		if addr != params.OwnerAddress {
			return true
		}
	}

	if params.Target != "" {
		if params.Target != tx.Target {
			return true
		}
	}

	if len(params.Tags) > 0 {
		// filter tags
		// Notice: exist same name tags
		filterTags := utils.TagsEncode(params.Tags)

		txTagsMap := make(map[string]struct{}) // key: name+value; value: {}
		for _, tg := range tx.Tags {
			txTagsMap[tg.Name+tg.Value] = struct{}{}
		}

		for _, ftg := range filterTags {
			if _, ok := txTagsMap[ftg.Name+ftg.Value]; !ok {
				return true
			}
		}
	}

	return false
}
func (s *Syncer) GetBlockByHeightRetry(height int64) (*types.Block, error) {
	return getBlockByHeightRetry(s.arClient, height, s.blockIdxs, s.peers)
}
func (s *Syncer) GetTxAndDataByIdRetry(height int64, txId string) (types.Transaction, error) {
	return getTxAndDataByIdRetry(height, s.arClient, txId, s.peers)
}
func (s *Syncer) GetBlockAndTxByIdRetry(height int64) (b *types.Block, subTxs []SubscribeTx, err error) {
	b, err = getBlockByHeightRetry(s.arClient, height, s.blockIdxs, s.peers)
	if err != nil {
		return
	}
	txs := mustGetTxs(height, b.Txs, s.arClient, int(s.conNum), s.peers)
	subTxs = make([]SubscribeTx, 0, len(txs))

	for _, tx := range txs {
		sTx := SubscribeTx{
			Transaction:    tx,
			BlockHeight:    b.Height,
			BlockId:        b.IndepHash,
			BlockTimestamp: b.Timestamp,
		}
		subTxs = append(subTxs, sTx)
	}
	return b, subTxs, nil
}
