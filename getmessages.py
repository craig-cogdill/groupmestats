#!/usr/bin/env python3

import argparse
from groupy.client import Client

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

def stream_messages_into_file(group, file_handle):
    count = 10
    messages_list = group.messages.list()
    for i in range(count):
        print("Results page "+str(i))
        for msg in messages_list:
            print(msg)
            file_handle.write(msg.name+","+str(msg.text)+","+str(len(msg.favorited_by))+"\n")
        oldest_message_in_page = messages_list[-1]
        messages_list = group.messages.list_before(oldest_message_in_page.id)


    #for i in range(count):
        

def dump_chat_history_to_file(group, filename):
    file_handle = open(filename, "w")
    #stream messages one page at a time
    stream_messages_into_file(group, file_handle)
    file_handle.close()

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
    #print_groups(client)
    if client is not None:
        stats_group = client.groups.get(input_args.groupid)
        stats_group.refresh_from_server()

        members = get_members(stats_group)
        print_members(members)
        dump_chat_history_to_file(stats_group, "out.csv")
        #all_messages = list(stats_group.messages.list().autopage())
        #print("size of all_messages: "+str(len(all_messages)))




        #message_list = stats_group.messages.list()
        #msgs = message_list.fetch_next()
        #print("num messages retrieved: "+str(len(msgs)))
        #for msg in msgs:
        #    print(msg.text)
        #    print(len(msg.favorited_by))

if __name__ == '__main__':
    input_args = sanitize_args()
    main(input_args)
