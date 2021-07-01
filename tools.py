def split_list(datas, chunk_size=10000):
    return [tuple(datas[x:x + chunk_size]) for x in range(0, len(datas), chunk_size)]
