package esstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"k8s.io/klog/v2"
)

type Index struct {
	client *elasticsearch.Client
}

func (s *Index) Search(ctx context.Context, query map[string]interface{}, indexName ...string) (*SearchResponse, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("error encoding query: %s", err)
	}
	res, err := s.client.Search(
		s.client.Search.WithContext(ctx),
		s.client.Search.WithIndex(indexName...),
		s.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, fmt.Errorf(res.String())
	}
	defer res.Body.Close()
	var r SearchResponse
	// TODO 这里是传&r还是r还是*r 这里需要测试一下
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *Index) DeleteByQuery(ctx context.Context, query map[string]interface{}, indexName ...string) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return fmt.Errorf("error encoding query: %s", err)
	}
	req := esapi.DeleteByQueryRequest{
		Index: indexName,
		Body:  &buf,
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	if res.IsError() {
		return fmt.Errorf("DeleteByQuery failure, response: %s", res.String())
	}
	return nil
}

func (s *Index) DeleteById(ctx context.Context, docId string, indexName string) error {
	req := esapi.DeleteRequest{
		Index:      indexName,
		DocumentID: docId,
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	if res.IsError() {
		return fmt.Errorf("delete failure, response: %v", res.String())
	}
	klog.V(4).Info("delete success, response: %v", res.String())
	return nil
}

func (s *Index) Upsert(ctx context.Context, indexName string, uid string, doc *Resource) error {
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal json error %v", err)
	}

	req := esapi.IndexRequest{
		DocumentID: uid,
		Body:       strings.NewReader(string(body)),
		Index:      indexName,
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	if res.IsError() {
		return fmt.Errorf(res.String())
	}
	klog.V(4).Info("upsert success, response: %v", res.String())
	return nil
}
