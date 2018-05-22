import re
import sys
input_path = "../input/user.json"
output_path = "../tmp/user_name_id_index"

def main():
    user_index = 0
    user_dict = {}
    with open(output_path, 'w') as f_w:
    with open(input_path, 'r') as f_r:
        for line in f_r:
            segs = line.strip()
            p1 = 'user_id": "(.*?)".*?name": "(.*?)"'
            pattern1 = re.compile(p1, re.S)
            information = pattern1.findall(segs)
            user_id = information[0][0]
            user_name = information[0][1]
            if not user_dict.has_key(user_id):
                user_dict[str(user_id)] = user_index
                user_index = user_index + 1
            f_w.write(user_id+','+str(user_dict.get(user_id))+','+user_name)
    f_w.close()
    f_r.close()
if __name__ == '__main__':
    sys.exit(main())
