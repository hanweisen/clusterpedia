package estest

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/clusterpedia-io/clusterpedia/pkg/storage/esstorage"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	"log"
	"strings"
	"testing"
)

var defaultPrefix = "kubernetes"

const index = "resources"

var mapping = `{
  "aliases": {
	"clusterpedia_resource": {}
  },
  "settings": {
    "index": {
      "number_of_shards": 1,
      "auto_expand_replicas": "0-1",
      "number_of_replicas": 0
    }
  },
  "mappings": {
    "properties": {
      "group": {
        "type": "keyword"
      },
      "version": {
        "type": "keyword"
      },
      "resource": {
        "type": "keyword"
      },
      "name": {
        "type": "keyword"
      },
      "namespace": {
        "type": "keyword"
      },
      "resource_version": {
        "type": "keyword"
      },
      "object": {
        "properties": {
          "metadata": {
            "properties": {
              "annotations": {
                "type": "flattened"
              },
              "creationTimestamp": {
                "type": "text"
              },
              "deletionTimestamp": {
                "type": "text"
              },
              "labels": {
                "type": "flattened"
              },
              "name": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "namespace": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "ownerReferences": {
                "type": "text"
              },
              "resourceVersion": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              }
            }
          },
          "spec":{
                "type":"object",
                "enabled":false
          }
        }
      }
    }
  }
}
`

// 文档id就使用uid吧
var mapping_bak20201031 = `{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "auto_expand_replicas": "0-1",
      "number_of_replicas": 0
    }
  },
  "mappings": {
    "properties": {
      "group": {
        "type": "keyword"
      },
      "version": {
        "type": "keyword"
      },
      "resource": {
        "type": "keyword"
      },
      "name": {
        "type": "keyword"
      },
      "namespace": {
        "type": "keyword"
      },
      "resource_version": {
        "type": "keyword"
      },
      "object": {
		"annotations":{
			"type":"object",
			"enabled":false
		},
		"name":{
			"type":"text",
			"fields":{
				"keyword":{
					"type":"keyword",
					"ignore_above":256
				}
			}
		}
      }
    }
  }
}`

// 文档id就使用uid吧
var mappingbak3 = `{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "auto_expand_replicas": "0-1",
      "number_of_replicas": 0
    }
  },
  "mappings": {
    "properties": {
      "group": {
        "type": "keyword"
      },
      "version": {
        "type": "keyword"
      },
      "resource": {
        "type": "keyword"
      },
      "name": {
        "type": "keyword"
      },
      "namespace": {
        "type": "keyword"
      },
      "resourceVersion": {
        "type": "keyword"
      },
      "object.apiVersion": {
        "type": "keyword"
      },
      "object.kind": {
        "type": "text"
      },
      "object.metadata.annotations": {
        "type": "flattened"
      },
      "object.metadata.creationTimestamp": {
        "type": "text"
      },
      "object.metadata.deletionTimestamp": {
        "type": "text"
      },
      "object.metadata.labels": {
        "type": "flattened"
      },
      "object.metadata.name": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "object.metadata.namespace": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "object.metadata.ownerReferences": {
        "type": "flattened"
      },
      "object.metadata.resourceVersion": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "object.spec": {
        "type": "flattened"
      },
      "object.data": {
        "type": "flattened"
      },
      "object.status": {
        "type": "flattened"
      }
    }
  }
}`

// 文档id就使用uid吧
var mapping_bak = `{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "auto_expand_replicas": "0-1",
      "number_of_replicas": 0
    }
  },
  "mappings": {
    "properties": {
      "apiVersion": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "kind": {
        "type": "keyword"
      },
      "metadata.annotations": {
        "type": "flattened"
      },
      "metadata.creationTimestamp": {
        "type": "text"
      },
      "metadata.deletionTimestamp": {
        "type": "text"
      },
      "metadata.labels": {
        "type": "flattened"
      },
      "metadata.name": {
        "type": "keyword",
      },
      "metadata.namespace": {
        "type": "keyword",
      },
      "metadata.ownerReferences": {
        "type": "flattened"
      },
      "metadata.resourceVersion": {
        "type": "keyword",
      },
      "spec": {
        "type": "flattened"
      },
      "status": {
        "type": "flattened"
      }
    }
  }
}`

var mappingbak2 = `{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "auto_expand_replicas": "0-1",
      "number_of_replicas": 0
    }
  },
  "mappings": {
    "properties": {
      "apiVersion": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "kind": {
        "type": "text"
      },
      "metadata.annotations": {
        "type": "flattened"
      },
      "metadata.creationTimestamp": {
        "type": "text"
      },
      "metadata.deletionTimestamp": {
        "type": "text"
      },
      "metadata.labels": {
        "type": "flattened"
      },
      "metadata.name": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "metadata.namespace": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "metadata.ownerReferences": {
        "type": "flattened"
      },
      "metadata.resourceVersion": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "spec": {
        "type": "flattened"
      },
      "status": {
        "type": "flattened"
      }
    }
  }
}`

//"spec": {
//"type": "flattened"
//},
var mappingbak = `{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    }
  },
  "mappings": {
    "properties": {
      "apiVersion": {
        "type": "text"
      },
      "kind": {
        "type": "text"
      },
      "metadata": {
        "properties": {
          "annotations": {
            "type": "flattened"
          },
          "creationTimestamp": {
            "type": "text"
          },
          "deletionTimestamp": {
            "type": "text"
          },
          "labels": {
            "type": "flattened"
          },
          "name": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "namespace": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "ownerReferences": {
            "type": "flattened"
          },
          "resourceVersion": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          }
        },
        "spec": {
          "type": "flattened"
        },
        "status": {
          "type": "flattened"
        }
      }
    }
  }
}
`

func getESClient() *elasticsearch.Client {
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		//Addresses: []string{"http://192.168.2.1:30200"},
		//Addresses: []string{"http://100.71.10.30:30200"},
		Addresses: []string{"http://100.71.10.46:9200"},
	})
	if err != nil {
		log.Fatalf("Error: NewClient(): %s", err)
	}
	return es
}

func TestIndicesExist(t *testing.T) {
	es := getESClient()
	res, err := es.Indices.Exists([]string{"hanweisen"})
	if err != nil {
		log.Fatalf("Error: Indices.Exists: %s", err)
	}
	res.Body.Close()
	log.Print(res.StatusCode)
}

func TestCreateIndicesAndDoc(t *testing.T) {
	es := getESClient()
	var b strings.Builder
	b.WriteString(`{"title" : "`)
	b.WriteString("doc1")
	b.WriteString(`"}`)
	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: "1",
		Body:       strings.NewReader(b.String()),
		Refresh:    "true",
	}
	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		log.Fatalf("error create index")
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and indexed document version.
			log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
		}
	}
}

func TestCreateMappingIndice(t *testing.T) {
	es := getESClient()
	req := esapi.IndicesCreateRequest{
		Index: index,
		Body:  strings.NewReader(mapping),
	}
	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	if res.IsError() {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and indexed document version.
			//log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
			log.Printf("%v", r)
		}
	} else {
		log.Printf("create index succeed")
	}
}

func TestCreateDoc(t *testing.T) {
	es := getESClient()
	spec := map[string]interface{}{
		"value": "key",
		"data":  "type223",
		"desc":  "I have a dream",
	}
	doc := map[string]interface{}{
		"apiVersion":                 "v1",
		"kind":                       "configmap",
		"metadata.name":              "mycm",
		"metadata.namespace":         "default",
		"metadata.creationTimestamp": "2022-08-08T06:26:15Z",
		//"metadata.labels":            "[]",
		//"metadata.annotations":       "[]",
		//"metadata.deletionTimestamp": "",
		"spec": spec,
	}
	body, err := json.Marshal(doc)
	if err != nil {
		log.Fatalf("marshal json error %v", err)
	}
	req := esapi.IndexRequest{
		Index: index,
		//DocumentID: string(us.GetUID()),
		Body: strings.NewReader(string(body)),
	}
	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("request fail %v", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		log.Printf("request err")
	}
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %s", err)
	} else {
		log.Printf("%v", r)

	}

}

func TestCreateDeploymentDoc(t *testing.T) {
	es := getESClient()
	dep := NewDeployment("default", "hanweisenTest")
	dep.ResourceVersion = "1"
	dep.ObjectMeta.Labels = map[string]string{
		//"shadow.clusterpedia.io/cluster-name": "cluster1",
		"label1": "value",
	}
	dep.Annotations = map[string]string{
		"shadow.clusterpedia.io/cluster-name": "cluster1",
		"createTime:":                         "2012-12",
		"creater:":                            "hanweisen",
	}
	dep.UID = types.UID(uuid.New().String())
	metaobj, err := meta.Accessor(dep)
	requestBody := map[string]interface{}{
		"group":    "apps",
		"version":  "v1",
		"resource": "deployments",
		"object":   metaobj,
	}

	body, err := json.Marshal(requestBody)
	if err != nil {
		log.Fatalf("marshal json error %v", err)
	}
	req := esapi.IndexRequest{
		DocumentID: string(dep.UID),
		Index:      index,
		Body:       strings.NewReader(string(body)),
	}
	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("request fail %v", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		log.Printf("request err")
	}
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %s", err)
	} else {
		log.Printf("%v", r)

	}

}

/*
func TestResultHandle(t *testing.T) {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": struct{}{},
		},
	}
	es := getESClient()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatalf("encode query err")
	}
	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex(index),
		es.Search.WithBody(&buf),
	)
	if err != nil {
		log.Fatalf("request err %v", err)
	}
	if res.IsError() {
		log.Fatalf("seearch failure %s", res.String())
	}
	defer res.Body.Close()
	var r esstorage.Result
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("decode err %v", err)
	}
	log.Printf("total record is %d", r.GetTotal())
	for _, item := range r.GetItems() {
		log.Printf("item: %v", item)
	}
}
*/

func TestDeleteCluster(t *testing.T) {
	es := getESClient()
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"metadata.annotations.shadow.clusterpedia.io/cluster-name": "cluster1",
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatalf("error encoding query: %s", err)
	}
	req := esapi.DeleteByQueryRequest{
		Index: []string{index},
		Body:  &buf,
	}
	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("call error %v", err)
	}
	if res.IsError() {
		log.Fatalf("request error %s", res.String())
	} else {
		defer res.Body.Close()
		log.Printf("request success")
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
		} else {
			log.Printf("delete result %v", r)

		}
	}
}

func TestDeleteClusterGVK(t *testing.T) {
	gvr := schema.GroupVersionResource{
		Group:    "apps",
		Version:  "v1",
		Resource: "deployments",
	}
	//https://github.com/kubecube-io/kubecube/blob/v1.2.5/pkg/conversion/conversion.go#L236
	//schema.FromAPIVersionAndKind()
	es := getESClient()
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"metadata.annotations.shadow.clusterpedia.io/cluster-name": "cluster1",
				"apiVersion": gvr.GroupVersion(),
				"kind":       gvr.Resource,
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatalf("error encoding query: %s", err)
	}
	req := esapi.DeleteByQueryRequest{
		Index: []string{index},
		Body:  &buf,
	}
	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("call error %v", err)
	}
	if res.IsError() {
		log.Fatalf("request error %s", res.String())
	} else {
		defer res.Body.Close()
		log.Printf("request success")
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
		} else {
			log.Printf("delete result %v", r)

		}
	}
}

// NewDeployment will build a deployment object.
func NewDeployment(namespace string, name string) *appsv1.Deployment {
	podLabels := map[string]string{"app": "nginx"}

	return &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: pointer.Int32Ptr(3),
			Selector: &metav1.LabelSelector{
				MatchLabels: podLabels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "nginx",
						Image: "nginx:1.19.0",
					}},
				},
			},
		},
	}
}

func TestIndexExist(t *testing.T) {
	es := getESClient()
	res, err := es.Indices.Exists([]string{index},
		es.Indices.Exists.WithContext(context.Background()))
	if err != nil {
		log.Fatalf("err %v", err)
	}
	if res.IsError() {
		log.Printf("res err %s", res.String())
	} else {
		log.Printf("result %s", res.String())
	}

}

func TestSearchResponse(t *testing.T) {
	es := getESClient()
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"resource": "configmaps",
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatalf("error encoding query: %s", err)
	}
	res, err := es.Search(
		es.Search.WithIndex(index),
		es.Search.WithBody(&buf),
	)
	if err != nil {
		log.Fatalf("%v", err)
	}
	if res.IsError() {
		log.Fatalf("%v", err)
	}
	var r esstorage.SearchResponse
	defer res.Body.Close()

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("%v", err)
	}
	println(r.Hits.Total.Value)
	println(r.Hits.Hits[0].Source.Name)
}

func TestSearchResponseString(t *testing.T) {
	es := getESClient()
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"resource": "configmaps",
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatalf("error encoding query: %s", err)
	}
	res, err := es.Search(
		es.Search.WithIndex(index),
		es.Search.WithBody(&buf),
	)
	if err != nil {
		log.Fatalf("%v", err)
	}
	if res.IsError() {
		log.Fatalf("%v", err)
	}
	log.Print(res.String())
}
