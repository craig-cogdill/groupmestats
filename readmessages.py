#!/usr/bin/env python3

import csv
import operator

def main():
    filename = "out.csv"

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

    print("----- LIKES -----")
    sorted_likes = sorted(member_likes.items(), key=operator.itemgetter(1), reverse=True)
    for name, likes in sorted_likes:
        print(name+":    "+str(likes))
    print("\n\n\n----- MSGS -----")
    sorted_msgs = sorted(member_messages.items(), key=operator.itemgetter(1), reverse=True)
    for name, num_msg in sorted_msgs:
        print(name+":    "+str(num_msg))

if __name__ == '__main__':
    main()
