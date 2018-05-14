import re
import sys
input_path = "../input/business.json"
output_path = "../input/business_name_id_index"

def main():
    business_index = 0
    business_dict = {}
    with open(output_path, 'w') as f_w:
    with open(input_path, 'r') as f_r:
        for line in f_r:
            segs = line.strip()
            p1 = 'business_id": "(.*?)".*?name": "(.*?)"'
            pattern1 = re.compile(p1, re.S)
            information = pattern1.findall(segs)
            business_id = information[0][0]
            business_name = information[0][1]
            if not business_dict.has_key(business_id):
                business_dict[str(business_id)] = business_index
                business_index = business_index + 1
            f_w.write(business_id+','+str(business_dict.get(business_id))+','+business_name)
    f_w.close()
    f_r.close()
if __name__ == '__main__':
    sys.exit(main())
