# python script that groups purchases of products by invoice number and flings
# each 'basket' to elasticsearch

import pandas as pd
import requests

# read in CSV
df = pd.read_csv('Online_Retail.csv')

# function to get current elasticsearch indices
def get_es_indices():
    r = requests.get("http://elasticsearch:9200/_cat/indices?format=json")
    if r.status_code != 200:
        print("Error listing indices")
        return None
    else:
        indices_full = r.json()  # contains full metadata as a dict
        indices = []  # let's extract the names separately
        for i in indices_full:
            indices.append(i['index'])
        return indices, indices_full
        

# function to create elastic search index 
def create_es_index(index, index_config):
    r = requests.put("http://elasticsearch:9200/{}".format(index),
                     json=index_config)
    if r.status_code != 200:
        print("Error creating index")
    else:
        print("Index created")
        
# function to delete elastic search index
def delete_es_index(index):
    r = requests.delete("http://elasticsearch:9200/{}".format(index))
    if r.status_code != 200:
        print("Error deleting index")
    else:
        print("Index deleted")
        
# function to send message to elasticsearch
def fling_message(index, doctype, msg):
    r = requests.post("http://elasticsearch:9200/{}/{}".format(index, doctype),
                      json=msg)
    if r.status_code != 201:
        print("Error sending message")
    else:
        print("message sent")
        
        
# delete index if it already exists and then create it
indices, indices_full = get_es_indices()
if 'baskets' in indices:
    delete_es_index('baskets')

# index config for the baskets     
index_config = {
    "mappings": {
        "basket": {  # document TYPE
            "properties": {
                "InvoiceNo": {"type": "string", "index": "not_analyzed"},
                "CustomerID": {"type": "string", "index": "not_analyzed"},
                "InvoiceDate": {"type": "string", "index": "not_analyzed"},
                "Country": {"type": "string", "index": "not_analyzed"},
                "StockCodes": {"type": "string"},
                "Descriptions": {"type": "string"},
                "Quantities": {"type": "string", "index": "not_analyzed"},
                "UnitPrices": {"type": "string", "index": "not_analyzed"}
            }
        }
    }
}


# create baskets index in elasticsearch
create_es_index('baskets', index_config)


# group item purchases by basket
# group by invoice num
invoice_groups = df.groupby('InvoiceNo')


# iterate over each group (each invoice)j to compile messages to fling
for invoice_name, invoice in invoice_groups:
    basket = {}
    stockcodes = []
    descriptions = []
    quantities = []
    unitprices = []
    # iterate over rows in this invoice dataframe
    for row_index, row in invoice.iterrows():
        # these fields are the same for each row, so doesn't matter if we keep overwriting
        basket['InvoiceNo'] = row['InvoiceNo']
        basket['CustomerID'] = row['CustomerID']
        basket['InvoiceDate'] = row['InvoiceDate']
        basket['Country'] = row['Country']
        # these fields are different for each row, so we append to lists
        stockcodes.append(row['StockCode'])
        descriptions.append(row['Description'])
        quantities.append(row['Quantity'])
        unitprices.append(row['UnitPrice'])
    basket['StockCodes'] = stockcodes
    basket['Descriptions'] = descriptions
    basket['Quantities'] = quantities
    basket['UnitPrices'] = unitprices
    msg = basket
    # fling messages to elasticsearch with fling function defined above
    fling_message('baskets', 'basket', msg)
