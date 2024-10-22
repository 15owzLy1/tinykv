package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/Connor1996/badger"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
    db := engine_util.CreateDB(conf.DBPath, false)
    return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

type StandAloneStorageReader struct {
    txn *badger.Txn
}

// GetCF 使用事务 txn 从指定列族 (CF) 中获取键值
func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
    item, err := r.txn.Get(engine_util.KeyWithCF(cf, key))
    if err != nil {
        if err == badger.ErrKeyNotFound {
            return nil, nil
        }
        return nil, err
    }
    val, err := item.ValueCopy(nil)
    return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
    return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
    r.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)  // 创建只读事务
    return &StandAloneStorageReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
    txn := s.db.NewTransaction(true)  // 创建写事务
    defer txn.Discard()

    for _, m := range batch {
        switch data := m.Data.(type) {
        case storage.Put:
            err := txn.Set(engine_util.KeyWithCF(data.Cf, data.Key), data.Value)
            if err != nil {
                return err
            }
        case storage.Delete:
            err := txn.Delete(engine_util.KeyWithCF(data.Cf, data.Key))
            if err != nil {
                return err
            }
        }
    }
    return txn.Commit()
}
