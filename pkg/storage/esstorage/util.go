package esstorage

import (
	"context"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"k8s.io/klog/v2"
	"strings"
)

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
