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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	genericstorage "k8s.io/apiserver/pkg/storage"
	"reflect"
	"strings"
)

type ResourceStorage struct {
	client *elasticsearch.Client
	codec  runtime.Codec

	storageGroupResource schema.GroupResource
	storageVersion       schema.GroupVersion
	memoryVersion        schema.GroupVersion

	indexName string
}

func (s *ResourceStorage) GetStorageConfig() *storage.ResourceStorageConfig {
	return &storage.ResourceStorageConfig{
		Codec:                s.codec,
		StorageGroupResource: s.storageGroupResource,
		StorageVersion:       s.storageVersion,
		MemoryVersion:        s.memoryVersion,
	}
}

func (s *ResourceStorage) Create(ctx context.Context, cluster string, obj runtime.Object) error {
	metaobj, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	/*
		annotations := metaobj.GetAnnotations()
		annotations["shadow.clusterpedia.io/cluster-name"] = cluster
		metaobj.SetAnnotations(annotations)
	*/
	doc := s.genDocument(metaobj)
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal json error %v", err)
	}

	req := esapi.IndexRequest{
		DocumentID: string(metaobj.GetUID()),
		Body:       strings.NewReader(string(body)),
		Index:      s.indexName,
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	return handleErrResponse(res)
}

func (s *ResourceStorage) List(ctx context.Context, listObject runtime.Object, opts *internal.ListOptions) error {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": struct{}{},
		},
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return fmt.Errorf("error encoding query: %s", err)
	}
	res, err := s.client.Search(
		s.client.Search.WithContext(ctx),
		s.client.Search.WithIndex(s.indexName),
		s.client.Search.WithBody(&buf),
	)
	if err != nil {
		return err
	}
	if res.IsError() {
		return fmt.Errorf(res.String())
	}
	defer res.Body.Close()
	var r Result
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return err
	}

	objects := make([]runtime.Object, r.GetTotal(), r.GetTotal())
	if unstructuredList, ok := listObject.(*unstructured.UnstructuredList); ok {
		for _, item := range r.GetItems() {
			object, exist := item["object"]
			if !exist {
				continue
			}
			uObj := &unstructured.Unstructured{}
			byte, err := json.Marshal(object)
			if err != nil {
				return err
			}
			obj, _, err := s.codec.Decode(byte, nil, uObj)
			if err != nil {
				return err
			}
			if obj != uObj {
				return fmt.Errorf("Failed to decode resource, into is %T", uObj)
			}
			objects = append(objects, uObj)

		}
		for _, object := range objects {
			if err != nil {
				return err
			}
			uObj, ok := object.(*unstructured.Unstructured)
			if !ok {
				return genericstorage.NewInternalError("the converted object is not *unstructured.Unstructured")
			}
			unstructuredList.Items = append(unstructuredList.Items, *uObj)
		}
		return nil
	}

	listPtr, err := meta.GetItemsPtr(listObject)
	if err != nil {
		return err
	}

	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		return fmt.Errorf("need ptr to slice: %v", err)
	}

	slice := reflect.MakeSlice(v.Type(), len(objects), len(objects))
	expected := reflect.New(v.Type().Elem()).Interface().(runtime.Object)

	for _, item := range r.GetItems() {
		object, exist := item["object"]
		if !exist {
			continue
		}
		into := expected.DeepCopyObject()
		byte, err := json.Marshal(object)
		if err != nil {
			return err
		}
		obj, _, err := s.codec.Decode(byte, nil, into)
		if err != nil {
			return err
		}
		if obj != into {
			return fmt.Errorf("Failed to decode resource, into is %T", into)
		}
		objects = append(objects, into)

	}
	for i, object := range objects {
		if err != nil {
			return err
		}
		slice.Index(i).Set(reflect.ValueOf(object).Elem())
	}
	v.Set(slice)
	return nil
}

func (s *ResourceStorage) Get(ctx context.Context, cluster, namespace, name string, into runtime.Object) error {
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"object.metadata.name":      name,
				"object.metadata.namespace": namespace,
				"object.metadata.annotations.shadow.clusterpedia.io/cluster-name": cluster,
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return fmt.Errorf("error encoding query: %s", err)
	}
	res, err := s.client.Search(
		s.client.Search.WithContext(ctx),
		s.client.Search.WithIndex(s.indexName),
		s.client.Search.WithBody(&buf),
	)
	if err != nil {
		return err
	}
	if res.IsError() {
		return fmt.Errorf(res.String())
	}
	defer res.Body.Close()
	var r Result
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return err
	}
	cnt := r.GetTotal()
	if cnt > 1 {
		//TODO return error
		return fmt.Errorf("find more than one item")
	}
	for _, item := range r.GetItems() {
		object, exist := item["object"]
		if !exist {
			continue
		}
		byte, err := json.Marshal(object)
		if err != nil {
			return err
		}
		obj, _, err := s.codec.Decode(byte, nil, into)
		if err != nil {
			return err
		}
		if obj != into {
			return fmt.Errorf("failed to decode resource, into is %v", into)
		}

	}
	return nil
}

func (s *ResourceStorage) Delete(ctx context.Context, cluster string, obj runtime.Object) error {
	metaobj, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	req := esapi.DeleteRequest{
		Index:      s.indexName,
		DocumentID: string(metaobj.GetUID()),
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	return handleErrResponse(res)
}

func (s *ResourceStorage) Update(ctx context.Context, cluster string, obj runtime.Object) error {
	return s.Create(ctx, cluster, obj)
}

func (s *ResourceStorage) genDocument(metaobj metav1.Object) map[string]interface{} {
	requestBody := map[string]interface{}{
		"group":           s.storageGroupResource.Group,
		"version":         s.memoryVersion.Version,
		"resource":        s.storageGroupResource.Resource,
		"name":            metaobj.GetName(),
		"namespace":       metaobj.GetNamespace(),
		"resourceVersion": metaobj.GetResourceVersion(),
		"object":          metaobj,
	}
	return requestBody
}

func handleErrResponse(response *esapi.Response) error {
	defer response.Body.Close()
	if response.IsError() {
		errMsg := response.String()
		return fmt.Errorf(errMsg)
	}
	return nil
}
