#!/usr/bin/env python3

import argparse
import sys
import datetime
from groupy.client import Client
from elasticsearch import Elasticsearch

HUNDRED_MSGS = 100

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
                    "attachments": {
                        "type": "long"
                    },
                    "msg": {
                        "type": "text",
                        "index": true
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
                    "nickname": {
                        "type": "keyword",
                        "index": true
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

def get_document_body(name, nickname, created_at, likes, liked_by, msg, attachments):
    body = {
       "name": name,
       "likes": likes,
       "created_at": created_at,
       "msg": msg,
       "nickname": nickname,
       "attachments": attachments,
       "liked_by": liked_by
    }
    return body

def print_groups(client):
    for group in client.groups.list():
        print("Group: "+group.name+"    "+str(group.id))

def print_members(members):
    for user_id,name in members.items():
        print("Name: "+name+"   ID: "+str(user_id))

def get_members(stats_group):
    members = {} 
    members_raw = stats_group.members
    for member in members_raw:
        members[member.user_id] = member.nickname
    return members

def get_formatted_member_string(members):
    ret = ""
    for user_id, name in members.items():
        ret = ret+str(user_id)+"|"+name+"\n"  
    return ret

def is_valid_member(members, user_id):
    return user_id in members

def convert_liked_by_to_names(liked_by_list):
    liked_by_names_list = []
    for user_id in liked_by_list:
        name = MEMBERS.get(user_id, 'unknown_user')
        liked_by_names_list.append(name)
    return liked_by_names_list

def stream_messages_into_file(group, members):
    es = Elasticsearch()
    if es is None:
        print("Couldn't create ES Client.")
        sys.exit(1)
    count = 1
    total_msgs = 0
    messages_list = group.messages.list(limit=100)
    while len(messages_list.items) != 0:
        sys.stdout.write("Saving results page {0}\r".format(count))
        sys.stdout.flush()
        for msg in messages_list:
            total_msgs = total_msgs + 1
            if is_valid_member(members, msg.user_id):
                name = MEMBERS[msg.user_id]
                nickname = msg.name
                num_likes = len(msg.favorited_by)
                #file_handle.write(msg.user_id+"|"+str(msg.created_at)+"|"+str(len(msg.favorited_by))+"\n")
                #print("-------------------------------")
                #print("Name: "+name)
                #print("Nickname: "+nickname)
                #d = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S+00:00')
                #index_name = "messages_"+str(d.year)+"_"+str(d.month)+"_"+str(d.day)
                #print("Created At: "+str(msg.created_at))
                #print("Likes: "+str(num_likes))
                # msg.created_by IS ALREADY A DATETIME
                d = msg.created_at
                #d = datetime.datetime.strptime(msg.created_at, '%Y-%m-%d %H:%M:%S+00:00')
                #index_name = "messages_"+str(d.year)+"_"+str(d.month)+"_"+str(d.day)
                index_name = "messages_"+str(d.year)
                if not es.indices.exists(index_name):
                    print("Creating index: "+index_name)
                    create_index(es, index_name)
                document_id = name+"_"+str(int(d.timestamp()))
                #print("DOC ID: " + document_id)
                #print("Favorited by: "+str(msg.favorited_by))
                liked_by_as_names_list = convert_liked_by_to_names(msg.favorited_by)
                doc_body = get_document_body(name=name, nickname=nickname, liked_by=liked_by_as_names_list, msg=msg.text, created_at=int(d.timestamp()), likes=num_likes, attachments=len(msg.attachments))
                #print("Body: " + str(doc_body))
                #print("-------------------------------")
                es.index(index=index_name, doc_type='messages', id=document_id, body=doc_body)

        oldest_message_in_page = messages_list[-1]
        messages_list = group.messages.list_before(message_id=oldest_message_in_page.id, limit=100)
        count = count + 1
    print("Flushed.  pages: "+str(count)+"   lines: "+str(total_msgs))

#def dump_chat_history_to_file(group, filename):
def dump_chat_history_to_file(group, members):
    #file_handle = open(filename, "w")
    print("Flushing chat messages to ElasticSearch...")
    stream_messages_into_file(group, members)
    #file_handle.close()

def sanitize_args():
    arg_parser = argparse.ArgumentParser(description="Get a list of messages from a GroupMe chat as CSV")
    arg_parser.add_argument("-t",
                            "--token",
                            dest="token",
                            metavar="TOKEN",
                            required=True,
                            help="GroupMe API access token")
    arg_parser.add_argument("-g",
                            "--groupid",
                            dest="groupid",
                            metavar="GROUP ID",
                            required=True,
                            help="Group ID for desired group stats")
    arg_parser.add_argument("-f",
                            "--file",
                            dest="file",
                            metavar="FILE",
                            help="CSV file to dump messages to")
    return arg_parser.parse_args()

def main(input_args):
    client = Client.from_token(input_args.token)
    if client is not None:
        stats_group = client.groups.get(input_args.groupid)
        print("Generating Stats for: "+stats_group.name)
        
        # Create a dict of members for name lookups 
        print("Getting group members...")
        members = get_members(stats_group)
        print_members(members)
        
        # One last server update before grabbing everything
        print("Refreshing groups...")
        stats_group.refresh_from_server()
        #dump_chat_history_to_file(stats_group, "out.csv")
        dump_chat_history_to_file(stats_group, members)

if __name__ == '__main__':
    input_args = sanitize_args()
    main(input_args)
