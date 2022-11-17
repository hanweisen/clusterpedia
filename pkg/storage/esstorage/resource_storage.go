package esstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/elastic/go-elasticsearch/v8"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	genericstorage "k8s.io/apiserver/pkg/storage"

	internal "github.com/clusterpedia-io/api/clusterpedia"
	"github.com/clusterpedia-io/clusterpedia/pkg/storage"
)

var (
	supportedOrderByFields = sets.NewString("cluster", "namespace", "name", "created_at", "resource_version")
)

// TODO 如何支持多种查询关键字
type CriteriaType int

type Operator int

type ResourceStorage struct {
	client *elasticsearch.Client
	codec  runtime.Codec

	storageGroupResource schema.GroupResource
	storageVersion       schema.GroupVersion
	memoryVersion        schema.GroupVersion

	indexName     string
	resourceAlias string

	index *Index
}

type QueryItem struct {
	criteriaType CriteriaType
	operator     Operator
	key          string
	criteria     interface{}
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
	// TODO 查询ownerid的功能放这里是否合适？？如果查询不到ownerIds又该如何处理？？
	ownerIds, err := s.GetOwnerIds(ctx, opts)
	if err != nil {
		return err
	}
	query, err := s.genListQuery(ownerIds, opts)
	if err != nil {
		return err
	}
	r, err := s.index.Search(ctx, query, s.indexName)
	if err != nil {
		return err
	}
	list, err := meta.ListAccessor(listObject)
	if err != nil {
		return err
	}
	offset, err := strconv.Atoi(opts.Continue)
	if opts.WithContinue != nil && *opts.WithContinue {
		if int64(len(r.GetResources())) == opts.Limit {
			list.SetContinue(strconv.FormatInt(int64(offset)+opts.Limit, 10))
		}
	}

	remain := r.GetTotal() - int64(offset) - int64(len(r.GetResources()))
	list.SetRemainingItemCount(&remain)

	objects := make([]runtime.Object, len(r.GetResources()))
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

func (s *ResourceStorage) GetOwnerIds(ctx context.Context, opts *internal.ListOptions) ([]string, error) {
	var empty []string
	switch {
	case len(opts.ClusterNames) != 1:
		return empty, nil
	case opts.OwnerUID != "":
		result, err := s.getUIDs(ctx, opts.ClusterNames[0], []string{opts.OwnerUID}, opts.OwnerSeniority)
		return result, err
	case opts.OwnerName != "":
		result, err := s.getUIDsByName(ctx, opts)
		return result, err

	default:
		return empty, nil
	}
}

func (s *ResourceStorage) getUIDsByName(ctx context.Context, opts *internal.ListOptions) ([]string, error) {
	cluster := opts.ClusterNames[0]
	var quayIts []*QueryItem
	if len(opts.Namespaces) != 0 {
		filter := &QueryItem{
			key:      "namespace",
			criteria: opts.Namespaces,
		}
		quayIts = append(quayIts, filter)
	}
	if !opts.OwnerGroupResource.Empty() {
		groupResource := opts.OwnerGroupResource
		groupFilter := &QueryItem{
			key:      "group",
			criteria: groupResource.Group,
		}
		resourceFilter := &QueryItem{
			key:      "resource",
			criteria: groupResource.Resource,
		}
		quayIts = append(quayIts, groupFilter, resourceFilter)
	}
	filter := &QueryItem{
		key:      "name",
		criteria: opts.OwnerName,
	}
	quayIts = append(quayIts, filter)
	filter = &QueryItem{
		key:      "object.metadata.annotations.shadow.clusterpedia.io/cluster-name",
		criteria: cluster,
	}
	quayIts = append(quayIts, filter)

	var mustFilters []map[string]interface{}
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
		mustFilters = append(mustFilters, criteria)
	}
	query := map[string]interface{}{
		"size":    500,
		"_source": []string{"object.metadata.uid"},
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": mustFilters,
			},
		},
	}
	r, err := s.index.Search(ctx, query, s.resourceAlias)
	if err != nil {
		return nil, err
	}
	var uids []string
	for _, resource := range r.GetResources() {
		// TODO 简单处理数据获取，这里可以转化成一个unstructured 对象进行进一步的处理
		object := resource.GetObject()
		uidmap := object["metadata"].(map[string]interface{})
		uid := uidmap["uid"].(string)
		uids = append(uids, uid)
	}
	return s.getUIDs(ctx, cluster, uids, opts.OwnerSeniority)
}

func (s *ResourceStorage) getUIDs(ctx context.Context, cluster string, uids []string, seniority int) ([]string, error) {
	if seniority == 0 {
		return uids, nil
	}
	query := map[string]interface{}{
		"size":    500,
		"_source": []string{"object.metadata.uid"},
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"terms": map[string]interface{}{
							"object.metadata.ownerReferences.uid": uids,
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

	r, err := s.index.Search(ctx, query, s.resourceAlias)
	if err != nil {
		return nil, err
	}
	uids = []string{}
	for _, resource := range r.GetResources() {
		// TODO 简单处理数据获取
		// 我需要测试一下能否直接反序列化
		object := resource.GetObject()
		uidmap := object["metadata"].(map[string]interface{})
		uid := uidmap["uid"].(string)
		uids = append(uids, uid)
	}
	return s.getUIDs(ctx, cluster, uids, seniority-1)
}

func (s *ResourceStorage) Get(ctx context.Context, cluster, namespace, name string, into runtime.Object) error {
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
	r, err := s.index.Search(ctx, query, s.indexName)
	if err != nil {
		return err
	}
	cnt := len(r.GetResources())
	if cnt > 1 {
		//TODO return error
		return fmt.Errorf("find more than one item")
	}
	if cnt == 0 {
		//TODO add not found error
		return fmt.Errorf("%s.%s \"%s\" not found", s.storageGroupResource.Resource, s.storageGroupResource.Group, name)
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
	if len(metaobj.GetUID()) == 0 {
		//TODO 会有一些脏数据存入es 导致后面删除报错
		return nil
	}
	err = s.index.DeleteById(ctx, string(metaobj.GetUID()), s.indexName)
	if err != nil {
		return err
	}
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
	err = s.index.Upsert(ctx, s.indexName, string(metaobj.GetUID()), resource)
	if err != nil {
		return err
	}
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
