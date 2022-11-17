package esstorage

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	ResourceConfigmap = "configmaps"
	ResourceSecret    = "secrets"
)

var mappingTemplate = `{
  "aliases": {
	"%s": {}
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
                "type": "date",
                "format": "yyyy-MM-dd'T'HH:mm:ss'Z'"
              },
              "deletionTimestamp": {
                "type": "keyword"
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
            }
          },
          %s
        }
      }
    }
  }
}`

var common = `
    "spec":{
        "type":"flattened",
        "ignore_above": 256
    }	
`
var configmap = `
    "data": {
        "type": "object",
	    "enabled":false
    },
    "binaryData": {
        "type": "object",
		"enabled":false
    }
`
var secret = `
    "data": {
        "type": "object",
        "enabled":false
    },
    "stringData": {
        "type": "object",
        "enabled":false
    }
`

func GetIndexMapping(alias string, storageGroupResource schema.GroupResource) string {
	resource := storageGroupResource.Resource
	switch resource {
	case ResourceConfigmap:
		return fmt.Sprintf(mappingTemplate, alias, configmap)
	case ResourceSecret:
		return fmt.Sprintf(mappingTemplate, alias, secret)
	default:
		return fmt.Sprintf(mappingTemplate, alias, common)
	}
}
