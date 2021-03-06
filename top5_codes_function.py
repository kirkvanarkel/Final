import requests
import pandas as pd

#function to get query from 'baskets' index
def execute_es_query(query):
    r = requests.get("http://elasticsearch:9200/baskets/_search",
                     json=query)
    #print(r.status_code)
    if r.status_code != 200:
        print("Error executing query")
        return None
    else:
        return r.json()



# A function with a query to get the top 5 recommended stock codes for a given stock code
def recommender_top5(StockCode):
    query = {
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"StockCodes": "{}".format(StockCode)}},
                ]
            }
        },
        "aggs": {
            "similar_products": {
                "significant_terms": {
                    "field": "StockCodes",
                    "exclude": "{}".format(StockCode),      # exclude stock code provided
                    "min_doc_count": 21,                    # more than 20 similar docs
                    "size": 5,                              # limit output to top 5
                }
            }
        }
    }
    res = execute_es_query(query)
    return pd.DataFrame(res['aggregations']['similar_products']['buckets'])


# Example of how to execute the query for a given stock code
print(recommender_top5(22045))
