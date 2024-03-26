package client

import (
	"context"
	"encoding/json"

	common "github.com/osiloke/gostore-common"
	"go-micro.dev/v4/client"

	proto "github.com/osiloke/gostore_raft/service/proto/store"
)

// NewStoreClient creates a new micro client for a cluster at advertiseName
func NewStoreClient(serviceName string, client client.Client, store common.ObjectStore) *StoreClient {
	return &StoreClient{
		serviceName: serviceName,
		client:      client,
		store:       store,
	}
}

// StoreClient is a go-micro client which proxies gostore methods to a cluster of nodes in a go-micro ecosystem
type StoreClient struct {
	client      client.Client
	serviceName string
	store       common.ObjectStore
}

func (s *StoreClient) Get(key string, store string, dst interface{}) error {
	ctx := context.Background()
	req := s.client.NewRequest(s.serviceName, "Store.Get", &proto.Request{Key: key, Store: store})
	rsp := proto.Response{}
	if err := s.client.Call(ctx, req, &rsp, client.WithRetries(3)); err != nil {
		return err
	}
	return json.Unmarshal([]byte(rsp.Val), &dst)
}

func (n *StoreClient) CreateDatabase() error {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) CreateTable(table string, sample interface{}) error {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) GetStore() interface{} {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) Stats(store string) (map[string]interface{}, error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) All(count int, skip int, store string) (common.ObjectRows, error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) AllCursor(store string) (common.ObjectRows, error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) Since(id string, count int, skip int, store string) (common.ObjectRows, error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) Before(id string, count int, skip int, store string) (common.ObjectRows, error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (int64, error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) Save(key string, store string, src interface{}) (string, error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) Update(key string, store string, src interface{}) error {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) Replace(key string, store string, src interface{}) error {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) Delete(key string, store string) error {
	panic("not implemented") // TODO: Implement
}

// GetTransaction(txn Transaction, key string, store string, dst interface{}) error
// SaveTransaction(txn Transaction, key, store string, src interface{}) (string, error)
// UpdateTransaction(txn Transaction, key string, store string, src interface{}) error
// ReplaceTransaction(txn Transaction, key string, store string, src interface{}) error
// DeleteTransaction(txn Transaction, key string, store string) error
func (n *StoreClient) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts common.ObjectStoreOptions) error {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts common.ObjectStoreOptions) error {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts common.ObjectStoreOptions) error {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) Query(filter map[string]interface{}, aggregates map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, common.AggregateResult, error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) FilterDelete(filter map[string]interface{}, store string, opts common.ObjectStoreOptions) error {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) FilterCount(filter map[string]interface{}, store string, opts common.ObjectStoreOptions) (int64, error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) GetByField(name string, val string, store string, dst interface{}) error {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) GetByFieldsByField(name string, val string, store string, fields []string, dst interface{}) (err error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) BatchDelete(ids []interface{}, store string, opts common.ObjectStoreOptions) (err error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) BatchUpdate(id []interface{}, data []interface{}, store string, opts common.ObjectStoreOptions) error {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) BatchFilterDelete(filter []map[string]interface{}, store string, opts common.ObjectStoreOptions) error {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) BatchInsert(data []interface{}, store string, opts common.ObjectStoreOptions) (keys []string, err error) {
	panic("not implemented") // TODO: Implement
}

func (n *StoreClient) Close() {
	panic("not implemented") // TODO: Implement
}

func (s *StoreClient) GetTX(key string, store string, dst interface{}, txn common.Transaction) error {
	panic("not implemented") // TODO: Implement
}

func (s *StoreClient) SaveTX(key string, store string, src interface{}, txn common.Transaction) error {
	panic("not implemented") // TODO: Implement
}

func (s *StoreClient) DeleteTX(key string, store string, tx common.Transaction) error {
	panic("not implemented") // TODO: Implement
}

func (s *StoreClient) FilterGetTX(filter map[string]interface{}, store string, dst interface{}, opts common.ObjectStoreOptions, tx common.Transaction) error {
	panic("not implemented") // TODO: Implement
}

func (s *StoreClient) BatchInsertTX(data []interface{}, store string, opts common.ObjectStoreOptions, tx common.Transaction) (keys []string, err error) {
	panic("not implemented") // TODO: Implement
}

func (s *StoreClient) UpdateTransaction() common.Transaction {
	panic("not implemented") // TODO: Implement
}

func (s *StoreClient) FinishTransaction(_ common.Transaction) error {
	panic("not implemented") // TODO: Implement
}
