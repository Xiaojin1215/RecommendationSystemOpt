import sys
import re

review_path = "../input/review.json"
user_path = "../tmp/user_name_id_index"
business_path = "../tmp/business_name_id_index"
output_path = "../tmp/review_id"

def main():
    user_dict = {}
    business_dict = {}
    with open(user_path, 'r') as f_u:
        data = f_u.readlines()
        for line in data:
            user_index_id = line.split(',')
            user_dict[str(user_index_id[1])] = int(user_index_id[0])
    f_u.close()
    with open(business_path, 'r') as f_b:
        data = f_b.readlines()
        for line in data:
            business_index_id = line.split(',')
            business_dict[str(business_index_id[1])] = int(business_index_id[0])
    f_b.close()
    with open(output_path, 'w') as f_w:
    with open(review_path, 'r') as f_r:
        for line in f_r:
            segs = line.strip().split(',')
            user_id = segs[1].split(':')
            user_id = user_id[1]
            user_id  user_id[1:-1]
            item_id = segs[2].split(':')
            item_id = item_id[1]
            item_id = item_id[1:-1]
            stars = segs[3].split(':')
            stars = stars[1]
            user_index = user_dict[str(user_id)]
            item_index = item_dict[str(item_id)]
            f_w.write(str(user_index)+','+str(item_index)+','+str(stars))
    f_w.close()
    f_r.close()
if __name__ == '__main__':
    sys.exit(main())

