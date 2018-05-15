import sys
import re
import random
import string

path_review = "../tmp/small_review"
last_user = 1326100
user_length = last_user + 1
gen_rate = 4
min_star = 1
max_star = 5

user_review = {}
with open(path_review, 'r') as fh2:
    for line in fh2:
        segs = line.strip().split(',')
        user_id = int(segs[0])
        item_id = int(segs[1])
        for i in range(1,gen_rate):
            new_user = i * user_length + user_id
            new_star = random.sample(range(min_star, max_star + 1), 1)
            new_star = new_star[0]
            print str(new_user) + "," + str(item_id) + "," + str(new_star)
