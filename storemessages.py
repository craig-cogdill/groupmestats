#!/usr/bin/env python3

import csv
import operator
import sys
import datetime
from elasticsearch import Elasticsearch

MEMBERS = {
    "20921361": "Tim",
    "10881133": "Josh",
    "14207333": "Ethan",
    "11831983": "Ben",
    "20921360": "Tanner",
    "10621961": "Travis",
    "14959401": "Craig",
    "16191547": "Aaron",
    "10881135": "Landon",
    "10881134": "Jordan",
}

def convert_epoch_to_timestamp(epoch_seconds):
    value = datetime.datetime.fromtimestamp(epoch_seconds)
    return value.strftime('%m-%d-%Y %H:%M:%S')

def get_new_index_mapping():
    mapping_string = '''
    {
        "mappings": {
            "messages": {
                "properties": {
                    "created_at": {
                        "type": "date",
                        "format": "MM-dd-YYYY HH:mm:ss||epoch_second"
                    },
                    "liked_by": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "likes": {
                        "type": "long"
                    },
                    "msg": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                              "type": "keyword",
                              "ignore_above": 256
                             }
                        }
                    },
                    "name": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    }                
                }
            }
        }
    }
    '''
    return mapping_string

def create_index(es, index_name):
    default_mapping = get_new_index_mapping()
    es.indices.create(index=index_name, body=default_mapping)

def get_document_body(name, created_at, likes):
    body = {
       "name": name,
       "likes": likes,
       "created_at": created_at 
    }
    return body

def main():
    filename = "out.csv"

    # --- ElasticSearch
    es = Elasticsearch()
    #index_name='messages_2018_12_22'
    #default_doc_type='messages'


    # Create index
    #default_mapping = get_new_index_mapping()
    #print("Default mapping = "+default_mapping)

    #if not es.indices.exists(index_name):
    #    print("INDEX DOES NOT EXIST")
    #    #Try creating it with a mapping

    #   es.indices.create(index=index_name, body=default_mapping)
    #   print("Index Created!!!")
    #else:
    #    print("Index exists!")
    #
    #print("Inserting doc...")
    #
    #es.index(index=index_name, doc_type=default_doc_type, id=2, body = {
    #    'name': 'craig',
    #    'created_at': '1545524182',
    #    'likes': 1,
    #    'msg':'helloworld',
    #    'liked_by': ['josh', 'aaron']
    #})



    #sys.exit(0)
    # --- Read File
    member_likes = {}
    member_messages = {}
    ctr = 0
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='|')
        for row in csv_reader:
            user_id = row[0]
            date_str = row[1]
            num_likes = row[2]
            name = MEMBERS[user_id]
            # 2015-08-26 16:34:23+00:00
            d = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S+00:00')
            index_name = "messages_"+str(d.year)+"_"+str(d.month)+"_"+str(d.day)
            if not es.indices.exists(index_name):
                print("Creating index: "+index_name)
                create_index(es, index_name)
            #print("User: "+MEMBERS[user_id])
            #print("Document:")
            document_id = name+"_"+str(int(d.timestamp()))
            #print("DOC ID: " + document_id)
            doc_body = get_document_body(name=MEMBERS[user_id], created_at=int(d.timestamp()), likes=num_likes)
            es.index(index=index_name, doc_type='messages', id=document_id, body=doc_body)
            ctr = ctr + 1


if __name__ == '__main__':
    main()
