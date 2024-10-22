package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/storage"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	// 创建一个新的读事务
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()  // 确保读事务在结束时关闭

	// 从指定列族 (CF) 获取键的值
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}

	// 构建响应，根据是否存在值设置 NotFound 字段
	resp := &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: val == nil,
	}

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
    // 创建一个 Modify 对象表示要存储的数据
    put := storage.Modify{
        Data: storage.Put{
            Cf:    req.Cf,
            Key:   req.Key,
            Value: req.Value,
        },
    }

    // 使用 Storage 接口的 Write 方法将数据写入存储
    err := server.storage.Write(req.Context, []storage.Modify{put})
    if err != nil {
        return nil, err
    }

    // 构建响应
    return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
    // 创建一个 Modify 对象表示要删除的数据
    del := storage.Modify{
        Data: storage.Delete{
            Cf:  req.Cf,
            Key: req.Key,
        },
    }

    // 使用 Storage 接口的 Write 方法删除数据
    err := server.storage.Write(req.Context, []storage.Modify{del})
    if err != nil {
        return nil, err
    }

    // 构建响应
    return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
    // 创建一个新的读事务
    reader, err := server.storage.Reader(req.Context)
    if err != nil {
        return nil, err
    }
    defer reader.Close()  // 确保读事务在结束时关闭

    // 创建迭代器，开始从指定的 start key 扫描
    iter := reader.IterCF(req.Cf)
    defer iter.Close()

    var kvPairs []*kvrpcpb.KvPair
    for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
        item := iter.Item()
        key := item.Key()

        // 获取值
        val, err := item.ValueCopy(nil)
        if err != nil {
            return nil, err
        }

        // 构造键值对
        kvPairs = append(kvPairs, &kvrpcpb.KvPair{
            Key:   key,
            Value: val,
        })

        // 达到限制时停止
        if len(kvPairs) >= int(req.Limit) {
            break
        }
    }

    // 构建响应
    return &kvrpcpb.RawScanResponse{
        Kvs: kvPairs,
    }, nil
}
