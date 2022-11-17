package esstorage

import (
	"context"
	"fmt"
	internal "github.com/clusterpedia-io/api/clusterpedia"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"k8s.io/klog/v2"
	"strconv"
	"strings"
)

func (s *ResourceStorage) genListQuery(ownerIds []string, opts *internal.ListOptions) (map[string]interface{}, error) {
	esQueryItems := newESQueryExpressionList()
	if opts.ClusterNames != nil {
		queryItem := newESQueryExpression(ClusterNamePath, opts.ClusterNames)
		esQueryItems = append(esQueryItems, queryItem)
	}
	if opts.Namespaces != nil {
		queryItem := newESQueryExpression(NamespacesPath, opts.Namespaces)
		esQueryItems = append(esQueryItems, queryItem)
	}
	if opts.Names != nil {
		queryItem := newESQueryExpression(NamePath, opts.Names)
		esQueryItems = append(esQueryItems, queryItem)
	}

	// 创建时间比较 created_at >= < ？？有点向同步时间？？
	//if opts.Since != nil && opts.Before != nil {
	//	TimeSet := []*metav1.Time{opts.Since, opts.Before}
	//	queryItem := newESQueryExpression(TimestampPath, TimeSet)
	//	queryItem.rangeFlag = true
	//	esQueryItems = append(esQueryItems, queryItem)
	//}

	// 原生sql的查询

	// LabelSelector查询
	if opts.LabelSelector != nil {
		if requirements, selectable := opts.LabelSelector.Requirements(); selectable {
			for _, requirement := range requirements {
				queryItem := labelQuery(requirement, false)
				esQueryItems = append(esQueryItems, queryItem)
			}
		}
	}

	// ExtraLabelSelector查询
	if opts.ExtraLabelSelector != nil {
		if requirements, selectable := opts.ExtraLabelSelector.Requirements(); selectable {
			for _, requirement := range requirements {
				queryItem := labelQuery(requirement, true)
				esQueryItems = append(esQueryItems, queryItem)
			}
		}
	}

	// FieldSelector的查询
	if opts.EnhancedFieldSelector != nil {
		if requirements, selectable := opts.EnhancedFieldSelector.Requirements(); selectable {
			for _, requirement := range requirements {
				queryItem, err := fieldQuery(requirement)
				if err != nil {
					return nil, err
				}
				esQueryItems = append(esQueryItems, queryItem)
			}
		}
	}

	// OwnerReference相关的查询
	if len(opts.ClusterNames) == 1 && (len(opts.OwnerUID) != 0 || len(opts.OwnerName) != 0) {
		queryItem := newESQueryExpression(UIDPath, ownerIds)
		esQueryItems = append(esQueryItems, queryItem)
	}

	esQueryItems = append(esQueryItems, newESQueryExpression(GroupPath, s.storageVersion.Group))
	esQueryItems = append(esQueryItems, newESQueryExpression(VersionPath, s.storageVersion.Version))
	esQueryItems = append(esQueryItems, newESQueryExpression(ResourcePath, s.storageGroupResource.Resource))

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

	query := Build(esQueryItems, size, offset)

	return query, nil
}

func EnsureIndex(client *elasticsearch.Client, mapping string, indexName string) error {
	req := esapi.IndicesCreateRequest{
		Index: indexName,
		Body:  strings.NewReader(mapping),
	}
	resp, err := req.Do(context.Background(), client)
	if err != nil {
		klog.Errorf("Error getting response: %v", err)
		return err
	}
	if resp.IsError() {
		msg := resp.String()
		if strings.Contains(resp.String(), "resource_already_exists_exception") {
			klog.Warningf("index %s already exists", indexName)
			return nil
		}
		return fmt.Errorf(msg)
	}
	klog.Infof("index %s created", indexName)
	return nil

}
