package esstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"

	internal "github.com/clusterpedia-io/api/clusterpedia"
	"github.com/clusterpedia-io/clusterpedia/pkg/storage"
)

const indexPrefix = "clusterpedia"

type StorageFactory struct {
	client     *elasticsearch.Client
	indexAlias string
}

func (s *StorageFactory) NewResourceStorage(config *storage.ResourceStorageConfig) (storage.ResourceStorage, error) {
	storage := &ResourceStorage{
		client: s.client,
		codec:  config.Codec,

		storageGroupResource: config.StorageGroupResource,
		storageVersion:       config.StorageVersion,
		memoryVersion:        config.MemoryVersion,
	}
	// indexAlias: ${prefix}-${group}-${resource}
	storage.indexName = fmt.Sprintf("%s-%s-%s", indexPrefix, config.StorageGroupResource.Group, config.StorageGroupResource.Resource)
	var mapping = GetIndexMapping(s.indexAlias, config.GroupResource)
	err := EnsureIndex(s.client, mapping, storage.indexName)
	if err != nil {
		return nil, err
	}
	return storage, nil
}

func (s *StorageFactory) NewCollectionResourceStorage(cr *internal.CollectionResource) (storage.CollectionResourceStorage, error) {
	return NewCollectionResourceStorage(s.client, s.indexAlias, cr), nil
}

func (s *StorageFactory) GetResourceVersions(ctx context.Context, cluster string) (map[schema.GroupVersionResource]map[string]interface{}, error) {
	resourceVersions := make(map[schema.GroupVersionResource]map[string]interface{})
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
		s.client.Search.WithIndex(s.indexAlias),
		s.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		if res.StatusCode == 404 {
			return resourceVersions, nil
		} else {
			return nil, fmt.Errorf(res.String())
		}
	}
	defer res.Body.Close()
	var r SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return nil, err
	}
	for _, item := range r.Hits.Hits {
		resource := item.Source
		gvr := resource.GroupVersionResource()
		versions := resourceVersions[gvr]
		if versions == nil {
			versions = make(map[string]interface{})
			resourceVersions[gvr] = versions
		}
		key := resource.GetName()
		if resource.GetNamespace() != "" {
			key = resource.GetNamespace() + "/" + resource.GetName()
		}
		versions[key] = resource.GetResourceVersion()
	}
	return resourceVersions, nil
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
		Index: []string{s.indexAlias},
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
		Index: []string{s.indexAlias},
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
	res, err := s.client.Indices.Exists([]string{s.indexAlias},
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
