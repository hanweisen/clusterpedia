package esstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	internal "github.com/clusterpedia-io/api/clusterpedia"
	"github.com/clusterpedia-io/clusterpedia/pkg/storage"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
)

type StorageFactory struct {
	client    *elasticsearch.Client
	indexName string
}

func (s *StorageFactory) NewResourceStorage(config *storage.ResourceStorageConfig) (storage.ResourceStorage, error) {

	return &ResourceStorage{
		client: s.client,
		codec:  config.Codec,

		storageGroupResource: config.StorageGroupResource,
		storageVersion:       config.StorageVersion,
		memoryVersion:        config.MemoryVersion,
		indexName:            s.indexName,
	}, nil
}

func (s *StorageFactory) NewCollectionResourceStorage(cr *internal.CollectionResource) (storage.CollectionResourceStorage, error) {
	return NewCollectionResourceStorage(s.client, s.indexName, cr), nil
}

func (s *StorageFactory) GetResourceVersions(ctx context.Context, cluster string) (map[schema.GroupVersionResource]map[string]interface{}, error) {
	resourceversions := make(map[schema.GroupVersionResource]map[string]interface{})
	var buf bytes.Buffer
	query := map[string]interface{}{
		"_source": []string{"group", "version", "resource", "namespace", "name", "resourceVersion"},
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"Object.metadata.annotations.shadow.clusterpedia.io/cluster-name": cluster,
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("error encoding query: %s", err)
	}
	res, err := s.client.Search(
		s.client.Search.WithContext(ctx),
		s.client.Search.WithIndex(s.indexName),
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
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return nil, err
	}
	for _, item := range r.Hits.Hits {
		resource := item.Source
		gvr := resource.GroupVersionResource()
		versions := resourceversions[gvr]
		if versions == nil {
			versions = make(map[string]interface{})
			resourceversions[gvr] = versions
		}
		key := resource.GetName()
		if resource.GetNamespace() != "" {
			key = resource.GetNamespace() + "/" + resource.GetName()
		}
		versions[key] = resource.GetResourceVersion()
	}
	return resourceversions, nil
}

func (s *StorageFactory) CleanCluster(ctx context.Context, cluster string) error {
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"Object.metadata.annotations.shadow.clusterpedia.io/cluster-name": cluster,
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return fmt.Errorf("error encoding query: %s", err)
	}
	req := esapi.DeleteByQueryRequest{
		Index: []string{"resource"},
		Body:  &buf,
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	if res.IsError() {
		return fmt.Errorf("clean cluster %v failure, response: %v", cluster, res.String())
	}
	klog.V(4).Info("clean cluster %v success, response: %v", cluster, res.String())
	return nil
}

func (s *StorageFactory) CleanClusterResource(ctx context.Context, cluster string, gvr schema.GroupVersionResource) error {
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"match": map[string]interface{}{
							"group": gvr.Group,
						},
					},
					{
						"match": map[string]interface{}{
							"version": gvr.Version,
						},
					},
					{
						"match": map[string]interface{}{
							"resource": gvr.Resource,
						},
					},
					{
						"match": map[string]interface{}{
							"Object.metadata.annotations.shadow.clusterpedia.io/cluster-name": cluster,
						},
					},
				},
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return fmt.Errorf("error encoding query: %s", err)
	}
	req := esapi.DeleteByQueryRequest{
		Index: []string{s.indexName},
		Body:  &buf,
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	if res.IsError() {
		return fmt.Errorf("clean cluster %s resource %s/%s failure, response: %s", cluster, gvr.GroupVersion(), gvr.Resource, res.String())
	}
	klog.V(4).Info("clean cluster %s resource %s/%s success, response: %s", cluster, gvr.GroupVersion(), gvr.Resource, res.String())
	return nil
}

func (s *StorageFactory) GetCollectionResources(ctx context.Context) ([]*internal.CollectionResource, error) {
	var crs []*internal.CollectionResource
	for _, cr := range collectionResources {
		crs = append(crs, cr.DeepCopy())
	}
	return crs, nil
}

func (s *StorageFactory) isIndexExist(ctx context.Context) (bool, error) {
	res, err := s.client.Indices.Exists([]string{s.indexName},
		s.client.Indices.Exists.WithContext(ctx))
	if err != nil {
		return false, err
	}
	if res.IsError() {
		if res.StatusCode == 404 {
			return false, nil
		} else {
			return false, fmt.Errorf(res.String())
		}
	}
	return true, nil
}
