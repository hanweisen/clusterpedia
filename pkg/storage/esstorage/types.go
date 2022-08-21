package esstorage

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type SearchResponse struct {
	Took    int  `json:"took"`
	TimeOut bool `json:"time_out"`
	Hits    Hits `json:"hits"`
}

type Hits struct {
	Total    Total   `json:"total"`
	MaxScore float32 `json:"max_score"`
	Hits     []Hit   `json:"hits"`
}

type Total struct {
	Value    int    `json:"value,omitempty"`
	Relation string `json:"relation,omitempty"`
}

type Hit struct {
	Index  string   `json:"_index"`
	Id     string   `json:"_id"`
	Score  float32  `json:"_score"`
	Source Resource `json:"_source"`
}

func (r *SearchResponse) GetTotal() int {
	return r.Hits.Total.Value
}

func (r *SearchResponse) GetResources() []*Resource {
	hits := r.Hits.Hits
	resources := make([]*Resource, len(hits))
	for i := range hits {
		resources[i] = &hits[i].Source
	}
	return resources
}

type Resource struct {
	Group           string `json:"group"`
	Version         string `json:"version"`
	Resource        string `json:"resource"`
	ResourceVersion string `json:"resource_version"`
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	Object          map[string]interface{}
}

func (r Resource) GroupVersionResource() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    r.Group,
		Version:  r.Version,
		Resource: r.Resource,
	}
}

func (r Resource) GetObject() map[string]interface{} {
	return r.Object
}
func (r Resource) GetName() string {
	return r.Name
}

func (r Resource) GetNamespace() string {
	return r.Namespace
}

func (r Resource) GetResourceVersion() string {
	return r.ResourceVersion
}
