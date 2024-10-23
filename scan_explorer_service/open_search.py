from typing import Dict, Iterator, List
import opensearchpy
from flask import current_app
from scan_explorer_service.utils.search_utils import EsFields, OrderOptions

def create_query_string_query(query_string: str):
    query =  {
        "query": {
            "query_string": {
                "query": query_string,
                "fields": ["article_bibcodes", "journal", "volume_id_lowercase", "volume"],
                "default_operator": "AND"
            }
        }
    }
    current_app.logger.debug(f"query string: {query}")
    return query

def append_aggregate(query: dict, agg_field: EsFields, page: int, size: int, sort: OrderOptions):
    from_number = (page - 1) * size
    query['size'] = 0
    if sort == OrderOptions.Bibcode_desc or sort == OrderOptions.Bibcode_asc:
        sort_field = '_key'
    elif sort == OrderOptions.Collection_desc or sort == OrderOptions.Collection_asc:
        sort_field = '_key'
    elif sort == OrderOptions.Relevance_desc or sort == OrderOptions.Relevance_asc:
        sort_field = 'score.sum'
    
    if "_desc" in sort.value:
        sort_order = "desc"
    else:
        sort_order = "asc"

    query['aggs'] = {   
        "total_count": {
            "cardinality": {
                "field": agg_field.value
            }
        },
        "ids": {
            "terms": {"field": agg_field.value,  "size": 10000},
            "aggs": {
                "bucket_sort": {
                    "bucket_sort": {
                        "sort": [{
                           sort_field: {
                                "order": sort_order
                            }
                        }],
                        "size": size,
                        "from": from_number
                    }
                }
            }
        }
    }

    if sort == OrderOptions.Relevance_desc or sort == OrderOptions.Relevance_asc:
        query['aggs']['ids']['aggs']['score'] = {"stats" : {"script" : "_score"}}

    return query

def append_highlight(query: dict):
    query['highlight'] = {
        "fields": {
            "text": {}
        },
        "type": "unified"
    }
    return query


def es_search(query: dict) -> Iterator[str]:
    es = opensearchpy.OpenSearch(current_app.config.get('OPEN_SEARCH_URL'))
    resp = es.search(index=current_app.config.get(
        'OPEN_SEARCH_INDEX'), body=query)
    return resp

def text_search_highlight(text: str, filter_field: EsFields, filter_value: str):
    query_string = text
    if filter_field:
        query_string += " " + filter_field.value + ":" + str(filter_value)
    base_query =  {
        "query": {
            "bool": {
                "must": {
                    "query_string": {
                        "query": query_string,
                        "default_field": "text",
                        "default_operator": "AND"
                    }
                },
            }
        }
    }
    query = set_page_search_fields(base_query)
    query = append_highlight(query)
    for hit in es_search(query)['hits']['hits']:
        yield {
            "page_id": hit['_source']['page_id'],
            "highlight": hit['highlight']['text']
        }

def set_page_ocr_fields(query: dict) -> dict:
    if '_source' in query.keys():
        query["_source"]["include"].append("text")
    else:
        query["_source"] = {"include": ["text"]}
    return query

def set_page_search_fields(query: dict) -> dict:
    query["_source"] = {"include": ["page_id", "volume_id", "page_label", "page_number"]}
    return query

def page_os_search(qs: str, page, limit, sort):
    query = create_query_string_query(qs)
    query = set_page_search_fields(query)
    from_number = (page - 1) * limit
    query['size'] = limit
    query['from'] = from_number
    query['track_total_hits'] = True

    if sort == OrderOptions.Bibcode_desc or sort == OrderOptions.Bibcode_asc:
        sort_field = 'article_bibcodes'
    elif sort == OrderOptions.Collection_desc or sort == OrderOptions.Collection_asc:
        sort_field = 'volume_id'
    elif sort == OrderOptions.Relevance_desc or sort == OrderOptions.Relevance_asc:
        sort_field = '_score'
    
    if "_desc" in sort.value:
        sort_order = "desc"
    else:
        sort_order = "asc"

    query['sort'] = [{sort_field:{'order': sort_order}}, {'page_number':{'order':'asc'}} ]
    es_result = es_search(query)
    return es_result

def page_ocr_os_search(collection_id: str, page_number:int):
    qs = EsFields.volume_id_lowercase + ":" + collection_id + " " + EsFields.page_number + ":" + str(page_number)
    query = create_query_string_query(qs)
    query = set_page_ocr_fields(query)
    es_result = es_search(query)
    return es_result

def aggregate_search(qs: str, aggregate_field, page, limit, sort):
    query = create_query_string_query(qs)
    current_app.logger.debug(f"query: {query}")
    query = append_aggregate(query, aggregate_field, page, limit, sort)
    current_app.logger.debug(f"query with aggregate: {query}")
    es_result = es_search(query)
    current_app.logger.debug(f"es_result: {es_result}")
    return es_result