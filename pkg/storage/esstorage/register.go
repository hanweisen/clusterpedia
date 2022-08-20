package esstorage

import (
	"github.com/clusterpedia-io/clusterpedia/pkg/storage"
	"github.com/elastic/go-elasticsearch/v8"
	"log"
)

const (
	StorageName = "elasticsearch"
)

func init() {
	storage.RegisterStorageFactoryFunc(StorageName, NewStorageFactory)
}

func NewStorageFactory(configPath string) (storage.StorageFactory, error) {
	return &StorageFactory{
		client:    getESClient(),
		indexName: "resources",
	}, nil
}

func getESClient() *elasticsearch.Client {
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		//Addresses: []string{"http://192.168.2.1:30200"},
		Addresses: []string{"http://10.211.55.4:30200"},
	})
	if err != nil {
		log.Fatalf("Error: NewClient(): %s", err)
	}
	return es
}
