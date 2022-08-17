package esstorage

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
