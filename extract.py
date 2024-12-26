import json_stream

cnt = 0

def visitor(item, path):
    global cnt
    cnt += 1
    if cnt % 1_000_000 == 0:
        print(f"{item} at path {path}")
    pass


if __name__ == '__main__':
    with open('./FloridaBlue_GBO_in-network-rates.json', 'r') as f:
        stream = json_stream.load(f)
        cnt = 0
        cpt = 0
        for obj in stream["in_network"].persistent():
            cnt += 1
            if obj["billing_code_type"] == "CPT":
                cpt += 1
            if cnt % 10_000 == 0:
                print(cnt, cpt)
