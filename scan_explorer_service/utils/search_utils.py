import math
from scan_explorer_service.models import PageType, PageColor
import shlex
import enum
import re

class SearchOptions(enum.Enum):
    """Available Search Options"""
    Bibcode = 'bibcode'
    Bibstem = 'bibstem'
    FullText = 'full'
    PageCollection = 'page_sequence'
    PageLabel = 'page'
    PageType = 'pagetype'
    PageColor = 'pagecolor'
    Project = 'project'
    Volume = 'volume'

class EsFields(str, enum.Enum):
    article_id = 'article_bibcodes'
    article_id_lowercase = 'article_bibcodes_lowercase'
    volume_id = 'volume_id'
    volume_id_lowercase = 'volume_id_lowercase'
    page_id = 'page_id'
    text = 'text'
    journal = 'journal'
    volume = 'volume_int'
    page_type = 'page_type'
    page_number = 'page_number'
    page_label = 'page_label'
    page_color = 'page_color'
    project = 'project'

query_translations = dict({
    SearchOptions.Bibstem.value: EsFields.journal.value,
    SearchOptions.Bibcode.value: EsFields.article_id_lowercase.value,
    SearchOptions.Volume.value: EsFields.volume.value,
    SearchOptions.PageType.value: EsFields.page_type.value,
    SearchOptions.PageCollection.value: EsFields.page_number.value,
    SearchOptions.PageLabel.value: EsFields.page_label.value,
    SearchOptions.PageColor.value: EsFields.page_color.value,
    SearchOptions.Project.value: EsFields.project.value,
    SearchOptions.FullText.value: EsFields.text.value,
})

class OrderOptions(str, enum.Enum):
    Relevance_desc = 'relevance_desc'
    Relevance_asc = 'relevance_asc'
    Bibcode_desc = 'bibcode_desc'
    Bibcode_asc = 'bibcode_asc'
    Collection_desc = 'collection_desc'
    Collection_asc = 'collection_asc'

def parse_query_args(args):
    qs = re.sub(':\s*', ':', args.get('q', '', str))
    qs, qs_dict = parse_query_string(qs)

    page = args.get('page', 1, int)
    limit = args.get('limit', 10, int)
    sort_raw = args.get('sort')
    sort = parse_sorting_option(sort_raw)
    return qs, qs_dict, page, limit, sort

def parse_query_string(qs):
    qs_to_split = qs.replace('[', '"[').replace(']',']"')
    qs_arr = [q for q in shlex.split(qs_to_split) if ':' in q]
    qs_dict = {}
    qs_only_free = qs
    
    for kv in qs_arr:
        kv_arr = kv.split(':', maxsplit=1)
        #Remove all parameter from the original search to be able to handle the free search
        qs_only_free = qs_only_free.replace(kv, "")
        if len(kv_arr) == 2:
            qs_dict[kv_arr[0].lower()] = kv_arr[1].strip()
            #If the option have quotes we remove them from the free. Previous removal would than have failed
            alt_kv = kv_arr[0] + ':"' + kv_arr[1] + '"'
            qs_only_free = qs_only_free.replace(alt_kv, '')

    check_query(qs_dict)
    #Adds a () around each free search to force OS to look for each individual entry against all default fields
    for parameter in re.split('\s+', qs_only_free):
        if parameter.upper() not in ['AND', 'OR', '']:
            qs = qs.replace(str(parameter), "(" + str(parameter) + ")")

    for key in qs_dict.keys():
        #Translate input on the keys to the dedicated OS columns
        insensitive_replace = re.compile(re.escape(key), re.IGNORECASE)
        qs = insensitive_replace.sub(query_translations[key.lower()], qs)   
        # To ensure only the strings after the colon are replaced and no partial replacements are made 
        insensitive_replace = re.compile(r'(?<=:)\b' + re.escape(qs_dict[key]) + r'\b', re.IGNORECASE)
        qs = insensitive_replace.sub(qs_dict[key], qs)
    return qs, qs_dict

def parse_sorting_option(sort_input: str):
    sort = OrderOptions.Bibcode_desc
    if sort_input:
        for sort_opt in OrderOptions:
            if sort_opt.value == sort_input.lower():
                sort = sort_opt
    return sort

def check_query(qs_dict: dict):
    """
        Checks that all queries have correct keys
    """
    for key in qs_dict.keys():
        # Will raise error if not in enum
        SearchOptions(key)
    check_page_type(qs_dict)
    check_page_color(qs_dict)
    check_project(qs_dict)

def check_page_type(qs_dict: dict): 
    if SearchOptions.PageType.value in qs_dict.keys():
        page_type = qs_dict[SearchOptions.PageType.value]
        valid_types = [p.name for p in PageType]
        if page_type in valid_types:
            return
        # Check lowercased and updated to cased
        for p in PageType:
            if page_type.replace('"','').lower() == p.name.lower():
                qs_dict[SearchOptions.PageType.value] = p.name
                return
        raise Exception("%s is not a valid page type, %s is possible choices"% (page_type, str(valid_types)))

def check_page_color(qs_dict: dict): 
    if SearchOptions.PageColor.value in qs_dict.keys():
        page_color = qs_dict[SearchOptions.PageColor.value]
        valid_types = [p.name for p in PageColor]
        if page_color in valid_types:
            return
        
        # Check lowercased and updated to cased
        for p in PageColor:
            if page_color.replace('"','').lower() == p.name.lower():
                qs_dict[SearchOptions.PageColor.value] = p.name
                return
        raise Exception("%s is not a valid page color, %s is possible choices"% (page_color, str(valid_types)))

def check_project(qs_dict: dict): 
    if SearchOptions.Project.value in qs_dict.keys():
        project = qs_dict[SearchOptions.Project.value]
        valid_types = ['PHaEDRA', 'Historical Literature', 'Microfilm Scanning']
        if project.replace('"','') in valid_types:
            qs_dict[SearchOptions.Project.value] =  project.replace('Microfilm Scanning', 'Historical Literature')
            return
        # Check lowercased and updated to cased
        for p in valid_types:
            if project.lower() == p.lower():
                qs_dict[SearchOptions.Project.value] = p.replace('Microfilm Scanning', 'Historical Literature')
                return
        raise Exception("%s is not a valid project, %s is possible choices"% (project, str(valid_types)))

def serialize_os_agg_page_bucket(bucket: dict):
    id = bucket['_source']['page_id']
    volume_id = bucket['_source']['volume_id']
    label = bucket['_source']['page_label']
    journal = volume_id[0:5]
    volume = volume_id[5:9]
    page_number = bucket['_source']['page_number']
    return {'id': id, 'collection_id':volume_id, 'journal': journal, 'volume': volume, 'label':label, 'volume_page_num': page_number}

def serialize_os_page_result(result: dict, page: int, limit: int, contentQuery):
    total_count = result['hits']['total']['value']
    page_count = int(math.ceil(min(total_count,10000) / limit))    
    es_buckets = result['hits']['hits']

    return {'page': page, 'pageCount': page_count, 'limit': limit, 'total': total_count, 'query': contentQuery,
        'items': [serialize_os_agg_page_bucket(b) for b in es_buckets]}

def serialize_os_page_ocr_result(result: dict):
    es_buckets = result['hits']['hits']
    if len(es_buckets) < 1:
        raise Exception("No page with those parameters found")
    return es_buckets[0]['_source']['text']

def serialize_os_agg_collection_bucket(bucket: dict):
    id = bucket['key']
    journal = id[0:5]
    volume = id[5:9]
    return {'id': id, 'journal': journal, 'volume': volume, 'pages': bucket['doc_count']}

def serialize_os_collection_result(result: dict, page: int, limit: int, contentQuery):
    total_count = result['aggregations']['total_count']['value']
    page_count = int(math.ceil(total_count / limit))    
    es_buckets = result['aggregations']['ids']['buckets']

    return {'page': page, 'pageCount': page_count, 'limit': limit, 'total': total_count, 'query': contentQuery,
        'items': [serialize_os_agg_collection_bucket(b) for b in es_buckets]}

def serialize_os_agg_article_bucket(bucket: dict):
    id = bucket['key']
    return {'id': id, 'bibcode': id, 'pages': bucket['doc_count']}

def serialize_os_article_result(result: dict, page: int, limit: int, contentQuery = '', extra_col_count = 0, extra_page_count = 0):
    total_count = result['aggregations']['total_count']['value']
    page_count = int(math.ceil(total_count / limit))    
    es_buckets = result['aggregations']['ids']['buckets']

    return {'page': page, 'pageCount': page_count, 'limit': limit, 'total': total_count, 'query': contentQuery,
        'extra_collection_count': extra_col_count, 'extra_page_count': extra_page_count,
        'items': [serialize_os_agg_article_bucket(b) for b in es_buckets]}

