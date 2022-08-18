package esstorage

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type Result map[string]interface{}

func (r Result) GetTotal() int {
	hits, ok := r["hits"].(map[string]interface{})
	if !ok {
		return 0
	}
	total, ok := hits["total"].(map[string]interface{})
	if !ok {
		return 0
	}
	// TODO 如何解析出value, 直接写死float64强转，感觉不太好
	value, ok := total["value"]
	if !ok {
		return 0
	}

	return int(value.(float64))
}

func (r Result) GetItems() []map[string]interface{} {
	var result = make([]map[string]interface{}, 0)
	hits, ok := r["hits"].(map[string]interface{})
	if !ok {
		return result
	}
	hitsInternal, ok := hits["hits"].([]interface{})
	if !ok {
		return result
	}
	for i := range hitsInternal {
		hit := hitsInternal[i].(map[string]interface{})
		source, ok := hit["_source"].(map[string]interface{})
		if ok {
			result = append(result, source)
		}
	}
	return result
}

type Resource map[string]interface{}

func (r Resource) GroupVersionResource() schema.GroupVersionResource {
	group := r["group"].(string)
	version := r["version"].(string)
	resource := r["resource"].(string)
	return schema.GroupVersionResource{
		Group:    group,
		Version:  version,
		Resource: resource,
	}
}

func (r Resource) GetObject() map[string]interface{} {
	return r["object"].(map[string]interface{})
}
func (r Resource) GetName() string {
	name := r["name"].(string)
	return name
}

func (r Resource) GetNamespace() string {
	namespace := r["namespace"].(string)
	return namespace
}

func (r Resource) GetResourceVersion() string {
	rv := r["resourceVersion"].(string)
	return rv
}
