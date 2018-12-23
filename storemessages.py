#!/usr/bin/env python3

import csv
import operator
import sys
import datetime
from elasticsearch import Elasticsearch

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

def main():
    filename = "out.csv"

    # --- ElasticSearch
    es = Elasticsearch()
    index_name='messages_2018_12_22'
    default_doc_type='messages'


    # Create index
    default_mapping = get_new_index_mapping()
    print("Default mapping = "+default_mapping)

    if not es.indices.exists(index_name):
        print("INDEX DOES NOT EXIST")
        #Try creating it with a mapping

        es.indices.create(index=index_name, body=default_mapping)
        print("Index Created!!!")
    else:
        print("Index exists!")
    
    print("Inserting doc...")

    es.index(index=index_name, doc_type=default_doc_type, id=2, body = {
        'name': 'craig',
        'created_at': '1545524182',
        'likes': 1,
        'msg':'helloworld',
        'liked_by': ['josh', 'aaron']
    })



    sys.exit(0)
    # --- Read File
    member_likes = {}
    member_messages = {}
    ctr = 0
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='|')
        for row in csv_reader:
            name = row[0]
            num_likes = row[2]
            #print(str(ctr)+": "+name+"  "+row[1])
            if name not in member_likes:
                member_likes[name] = 0
            if name not in member_messages:
                member_messages[name] = 0
            member_messages[name] = member_messages[name] + 1
            member_likes[name] = member_likes[name] + int(num_likes)
            ctr = ctr + 1

    es.index(index=indexname, doc_type='post', id=1, body = {
        'name': 'craig',
        'created_at': 1545519287,
        'likes': 1,
        'msg':'helloworld'
    })

if __name__ == '__main__':
    main()
