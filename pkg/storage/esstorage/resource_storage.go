package esstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	internal "github.com/clusterpedia-io/api/clusterpedia"
	"github.com/clusterpedia-io/clusterpedia/pkg/storage"
	"github.com/clusterpedia-io/clusterpedia/pkg/storage/internalstorage"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	genericstorage "k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"
	"reflect"
	"strconv"
	"strings"
)

var (
	supportedOrderByFields = sets.NewString("cluster", "namespace", "name", "created_at", "resource_version")
)

// TODO 如何支持多种查询关键字
type CriteriaType int

const (
	Term = iota
	Fuzzy
	Match
)

type Operator int

const (
	Equal = iota
	notEqual
)

type ResourceStorage struct {
	client *elasticsearch.Client
	codec  runtime.Codec

	storageGroupResource schema.GroupResource
	storageVersion       schema.GroupVersion
	memoryVersion        schema.GroupVersion

	indexName     string
	resourceAlias string
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

func (s *ResourceStorage) genListQuery(ownerIds []string, opts *internal.ListOptions) (map[string]interface{}, error) {
	var quayIts []*QueryItem
	// 这篇文档搞定in https://www.elastic.co/guide/cn/elasticsearch/guide/current/_finding_multiple_exact_values.html
	switch len(opts.ClusterNames) {
	case 0:
	default:
		item := &QueryItem{
			key:      "object.metadata.annotations.shadow.clusterpedia.io/cluster-name",
			criteria: opts.ClusterNames,
		}
		quayIts = append(quayIts, item)
	}
	switch len(opts.Namespaces) {
	case 0:
	default:
		item := &QueryItem{
			key:      "namespace",
			criteria: opts.Namespaces,
		}
		quayIts = append(quayIts, item)
	}
	switch len(opts.Names) {
	case 0:
	default:
		item := &QueryItem{
			key:      "name",
			criteria: opts.Names,
		}
		quayIts = append(quayIts, item)
	}

	// 创建时间比较 created_at >= < ？？有点向同步时间？？
	if opts.Since != nil && opts.Before != nil {
		path := "object.metadata.creationTimestamp"
		query := map[string]interface{}{
			"query": map[string]interface{}{
				"range": map[string]interface{}{
					path: map[string]interface{}{
						"gte": opts.Since.Time.UTC().Unix(),
						"lte": opts.Before.Time.UTC().Unix(),
					},
				},
			},
		}
		return query, nil
	}

	// 原生sql的查询

	// LabelSelector的查询
	if opts.LabelSelector != nil {
		if requirements, selectable := opts.LabelSelector.Requirements(); selectable {
			for _, requirement := range requirements {
				path := "object.metadata.labels"
				values := requirement.Values().List()
				var item *QueryItem
				switch requirement.Operator() {
				case selection.Exists:
				case selection.DoesNotExist:
				case selection.Equals, selection.DoubleEquals:
					item = &QueryItem{
						key:      path,
						criteria: values[0],
					}
				case selection.NotEquals:
					item = &QueryItem{
						operator: notEqual,
						key:      path,
						criteria: values[0],
					}
				case selection.In:
					item = &QueryItem{
						key:      path,
						criteria: values,
					}
				case selection.NotIn:
					item = &QueryItem{
						operator: notEqual,
						key:      path,
						criteria: values,
					}
				default:
					continue
				}
				quayIts = append(quayIts, item)
			}
		}
	}

	// opts.ExtraLabelSelector
	if opts.ExtraLabelSelector != nil {
		if requirements, selectable := opts.ExtraLabelSelector.Requirements(); selectable {
			for _, require := range requirements {
				switch require.Key() {
				case internalstorage.SearchLabelFuzzyName:
					for _, name := range require.Values().List() {
						name = strings.TrimSpace(name)
						item := &QueryItem{
							criteriaType: Fuzzy,
							key:          "name",
							criteria:     name,
						}
						quayIts = append(quayIts, item)
					}
				}
			}
		}
	}

	// fieldSelector的查询
	if opts.EnhancedFieldSelector != nil {
		if requirements, selectable := opts.EnhancedFieldSelector.Requirements(); selectable {
			for _, requirement := range requirements {
				var (
					fields      []string
					fieldErrors field.ErrorList
				)
				for _, f := range requirement.Fields() {
					if f.IsList() {
						fieldErrors = append(fieldErrors, field.Invalid(f.Path(), f.Name(), fmt.Sprintf("Storage<%s>: Not Support list field", StorageName)))
						continue
					}
					fields = append(fields, f.Name())
				}
				if len(fieldErrors) != 0 {
					return nil, apierrors.NewInvalid(schema.GroupKind{Group: internal.GroupName, Kind: "ListOptions"}, "fieldSelector", fieldErrors)
				}
				fields = append(fields, "")
				copy(fields[1:], fields[0:])
				fields[0] = "object"
				path := strings.Join(fields, ".")
				values := requirement.Values().List()
				var item *QueryItem
				switch requirement.Operator() {
				case selection.Exists:
				case selection.DoesNotExist:
				case selection.Equals, selection.DoubleEquals:
					item = &QueryItem{
						key:      path,
						criteria: values[0],
					}
				case selection.NotEquals:
					item = &QueryItem{
						operator: notEqual,
						key:      path,
						criteria: values[0],
					}
				case selection.In:
					item = &QueryItem{
						key:      path,
						criteria: values,
					}
				case selection.NotIn:
					item = &QueryItem{
						operator: notEqual,
						key:      path,
						criteria: values,
					}
				default:
					continue
				}
				quayIts = append(quayIts, item)
			}
		}
	}

	// ownerfeference相关的查询
	if len(opts.ClusterNames) == 1 && (len(opts.OwnerUID) != 0 || len(opts.OwnerName) != 0) {
		item := &QueryItem{
			key:      "object.metadata.ownerReferences.uid",
			criteria: ownerIds,
		}
		quayIts = append(quayIts, item)
	}
	item := &QueryItem{
		key:      "version",
		criteria: s.storageVersion.Version,
	}
	quayIts = append(quayIts, item)
	item = &QueryItem{
		key:      "group",
		criteria: s.storageVersion.Group,
	}
	quayIts = append(quayIts, item)
	item = &QueryItem{
		key:      "resource",
		criteria: s.storageGroupResource.Resource,
	}

	quayIts = append(quayIts, item)
	var mustFilter []map[string]interface{}
	var notFilter []map[string]interface{}
	for i := range quayIts {
		var word string
		if quayIts[i].criteriaType == Fuzzy {
			word = "fuzzy"
		} else {
			if _, ok := quayIts[i].criteria.([]string); ok {
				word = "terms"
			} else {
				word = "term"
			}
		}
		criteria := map[string]interface{}{
			word: map[string]interface{}{
				quayIts[i].key: quayIts[i].criteria,
			},
		}
		if quayIts[i].operator == Equal {
			mustFilter = append(mustFilter, criteria)
		} else {
			notFilter = append(notFilter, criteria)
		}
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
				"must":     mustFilter,
				"must_not": notFilter,
			},
		},
	}
	return query, nil
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

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("error encoding query: %s", err)
	}
	res, err := s.client.Search(
		s.client.Search.WithContext(ctx),
		s.client.Search.WithIndex(s.resourceAlias),
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
	var uids []string
	for _, resource := range r.GetResources() {
		// TODO 简单处理数据获取，
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
	var buf bytes.Buffer
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
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("error encoding query: %s", err)
	}
	res, err := s.client.Search(
		s.client.Search.WithContext(ctx),
		s.client.Search.WithIndex(s.resourceAlias),
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
	uids = []string{}
	for _, resource := range r.GetResources() {
		// TODO 简单处理数据获取，
		object := resource.GetObject()
		uidmap := object["metadata"].(map[string]interface{})
		uid := uidmap["uid"].(string)
		uids = append(uids, uid)
	}
	return s.getUIDs(ctx, cluster, uids, seniority-1)
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
	cnt := len(r.GetResources())
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
