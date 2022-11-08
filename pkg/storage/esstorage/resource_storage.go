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
	"k8s.io/apimachinery/pkg/util/sets"
	genericstorage "k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"
	"reflect"
	"strconv"
	"strings"
)

var (
	supportedOrderByFields = sets.NewString("cluster", "namespace", "name", "created_at", "resource_version")
)

type ResourceStorage struct {
	client *elasticsearch.Client
	codec  runtime.Codec

	storageGroupResource schema.GroupResource
	storageVersion       schema.GroupVersion
	memoryVersion        schema.GroupVersion

	indexName string
}

type QueryItem struct {
	key      string
	criteria interface{}
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
	return s.upsert(ctx, cluster, obj)
}

func (s *ResourceStorage) List(ctx context.Context, listObject runtime.Object, opts *internal.ListOptions) error {
	// TODO
	query := s.genListQuery(opts)
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
	var r SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return err
	}

	objects := make([]runtime.Object, r.GetTotal())
	if unstructuredList, ok := listObject.(*unstructured.UnstructuredList); ok {
		for _, resource := range r.GetResources() {
			object := resource.GetObject()
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
				return genericstorage.NewInternalError("the converted Object is not *unstructured.Unstructured")
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

	for i, resource := range r.GetResources() {
		object := resource.GetObject()
		byte, err := json.Marshal(object)
		if err != nil {
			return err
		}
		obj, _, err := s.codec.Decode(byte, nil, expected.DeepCopyObject())
		if err != nil {
			return err
		}
		slice.Index(i).Set(reflect.ValueOf(obj).Elem())

	}
	v.Set(slice)
	return nil
}

func (s *ResourceStorage) genListQuery(opts *internal.ListOptions) map[string]interface{} {
	var quayIts []*QueryItem
	// 这篇文档搞定in https://www.elastic.co/guide/cn/elasticsearch/guide/current/_finding_multiple_exact_values.html
	// cluster的精确与in
	switch len(opts.ClusterNames) {
	case 0:
	case 1:
		//query = query.Where("cluster = ?", opts.ClusterNames[0])
		item := &QueryItem{
			key:      "object.metadata.annotations.shadow.clusterpedia.io/cluster-name",
			criteria: opts.ClusterNames[0],
		}
		quayIts = append(quayIts, item)
	default:
		//query = query.Where("cluster IN ?", opts.ClusterNames)
		item := &QueryItem{
			key:      "object.metadata.annotations.shadow.clusterpedia.io/cluster-name",
			criteria: opts.ClusterNames,
		}
		quayIts = append(quayIts, item)
	}
	// namespace的精确与in
	switch len(opts.Namespaces) {
	case 0:
	case 1:
		item := &QueryItem{
			key:      "namespace",
			criteria: opts.Namespaces[0],
		}
		quayIts = append(quayIts, item)
	default:
		item := &QueryItem{
			key:      "namespace",
			criteria: opts.Namespaces,
		}
		quayIts = append(quayIts, item)
	}
	// name的精确与in
	switch len(opts.Names) {
	case 0:
	case 1:
		item := &QueryItem{
			key:      "name",
			criteria: opts.Names[0],
		}
		quayIts = append(quayIts, item)
	default:
		item := &QueryItem{
			key:      "name",
			criteria: opts.Names,
		}
		quayIts = append(quayIts, item)
	}
	// 创建时间比较 created_at >= < ？？有点向同步时间？？
	// 原生sql的查询
	// LabelSelector的查询
	// opts.ExtraLabelSelector 我不知道是啥
	// fieldSelector的查询
	// ownerfeference相关的查询，这个目前还做不到！！

	item := &QueryItem{
		key:      "version",
		criteria: s.storageVersion.Version,
	}
	quayIts = append(quayIts, item)
	var critters []map[string]interface{}
	for i := range quayIts {
		var word string
		if _, ok := quayIts[i].criteria.([]string); ok {
			word = "terms"
		} else {
			word = "term"
		}
		criteria := map[string]interface{}{
			word: map[string]interface{}{
				quayIts[i].key: quayIts[i].criteria,
			},
		}
		critters = append(critters, criteria)
	}
	// 设置排序，limit与offset
	size := 500
	if opts.Limit != -1 {
		size = int(opts.Limit)
	}
	offset, _ := strconv.Atoi(opts.Continue)

	for _, orderby := range opts.OrderBy {
		field := orderby.Field
		if supportedOrderByFields.Has(field) {
			//TODO add sort
		}
	}

	query := map[string]interface{}{
		"size": size,
		"from": offset,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": critters,
			},
		},
	}
	return query
}

func (s *ResourceStorage) Get(ctx context.Context, cluster, namespace, name string, into runtime.Object) error {
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"group": s.storageGroupResource.Group,
						},
					},
					{
						"term": map[string]interface{}{
							"version": s.storageVersion.Version,
						},
					},
					{
						"term": map[string]interface{}{
							"resource": s.storageGroupResource.Resource,
						},
					},
					{
						"term": map[string]interface{}{
							"object.metadata.name": name,
						},
					},
					{
						"term": map[string]interface{}{
							"object.metadata.namespace": namespace,
						},
					},
					{
						"term": map[string]interface{}{
							"object.metadata.annotations.shadow.clusterpedia.io/cluster-name": cluster,
						},
					},
				},
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
	var r SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return err
	}
	cnt := r.GetTotal()
	if cnt > 1 {
		//TODO return error
		return fmt.Errorf("find more than one item")
	}
	if cnt == 0 {
		return fmt.Errorf("can not fi")
	}
	for _, resource := range r.GetResources() {
		object := resource.Object

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
	if res.IsError() {
		return fmt.Errorf("delete failure, response: %v", res.String())
	}
	klog.V(4).Info("delete success, response: %v", res.String())
	return nil
}

func (s *ResourceStorage) Update(ctx context.Context, cluster string, obj runtime.Object) error {
	return s.upsert(ctx, cluster, obj)
}

func (s *ResourceStorage) upsert(ctx context.Context, cluster string, obj runtime.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind == "" {
		return fmt.Errorf("%s: kind is required", gvk)
	}
	metaobj, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	resource := s.genDocument(metaobj, gvk)
	body, err := json.Marshal(resource)
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

	if res.IsError() {
		msg := fmt.Sprintf("upsert %s/%s %s/%s failure, response: %v", s.storageGroupResource.Group, s.storageGroupResource.Resource, metaobj.GetNamespace(), metaobj.GetName(), res.String())
		klog.Error(msg)
		return fmt.Errorf(msg)
	}
	klog.V(4).Info("upsert success, response: %v", res.String())
	return nil
}

func (s *ResourceStorage) genDocument(metaobj metav1.Object, gvk schema.GroupVersionKind) map[string]interface{} {
	requestBody := map[string]interface{}{
		"group":           s.storageGroupResource.Group,
		"version":         s.storageVersion.Version,
		"resource":        s.storageGroupResource.Resource,
		"kind":            gvk.Kind,
		"name":            metaobj.GetName(),
		"namespace":       metaobj.GetNamespace(),
		"resourceVersion": metaobj.GetResourceVersion(),
		"object":          metaobj,
	}
	return requestBody
}
