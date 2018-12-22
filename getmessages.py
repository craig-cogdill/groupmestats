#!/usr/bin/env python3

import argparse
import sys
from groupy.client import Client

HUNDRED_MSGS = 100

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

def stream_messages_into_file(group, members, file_handle):
    count = 1
    total_msgs = 0
    messages_list = group.messages.list(limit=100)
    while len(messages_list.items) != 0:
        sys.stdout.write("Saving results page {0}\r".format(count))
        sys.stdout.flush()
        for msg in messages_list:
            total_msgs = total_msgs + 1
            if is_valid_member(members, msg.user_id):
                file_handle.write(msg.user_id+"|"+str(msg.created_at)+"|"+str(len(msg.favorited_by))+"\n")
        oldest_message_in_page = messages_list[-1]
        messages_list = group.messages.list_before(message_id=oldest_message_in_page.id, limit=100)
        count = count + 1
    print("Flushed.  pages: "+str(count)+"   lines: "+str(total_msgs))

def dump_chat_history_to_file(group, filename):
    file_handle = open(filename, "w")
    print("Getting group members...")
    members = get_members(group)
    #file_handle.write(get_formatted_member_string(members))
    print("Flushing chat messages to file...")
    stream_messages_into_file(group, members, file_handle)
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
    if client is not None:
        stats_group = client.groups.get(input_args.groupid)
        
        # Comment me out
        print_members(get_members(stats_group))
        
        #print("Refreshing groups...")
        #stats_group.refresh_from_server()
        #dump_chat_history_to_file(stats_group, "out.csv")

if __name__ == '__main__':
    input_args = sanitize_args()
    main(input_args)
