import json
import os

from elasticsearch import Elasticsearch


def load_docs_from_es(size, host='localhost:9200', query=''):
    SIZE = min(200, size)
    es = Elasticsearch([host])
    body = {
        "_source": ["raw.abstract", "raw.doi"],
        "query": {
          "bool": ({
            "must": [
              {
                "match": {
                  "raw.keywords": {
                    "query": query
                  }
                }
              },
            ],
          }) if query else {}
        }
    }
    res = es.search(
        index="pubmed", size=SIZE, body=body, scroll='1h'
    )
    sid = res['_scroll_id']
    scroll_size = res['hits']['total']
    while (scroll_size > 0 and size > 0):
        for r in res['hits']['hits']:
            yield (r['_source']['raw']['doi'], r['_source']['raw']['abstract'])
        res = es.scroll(scroll_id=sid, scroll='1h')
        sid = res['_scroll_id']
        scroll_size = len(res['hits']['hits'])
        size -= SIZE


def load_docs_from_dir(size, dir_path):
    files = [os.path.join(p, f)
             for p, _, fs in os.walk(dir_path)
             for f in fs if f and f.endswith('.json')][:size]
    for f_path in files:
        doc = json.load(open(f_path))
        yield (doc['raw']['doi'], doc['raw']['abstract'])
