from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import sys

def chunks(file, chunk_size):
    chunk = []
    for i, line in enumerate(file):
        chunk.append(line)
        if (i + 1) % chunk_size == 0:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def count_words(lines):
    local_count = defaultdict(int)
    for l in lines:
        for w in l.split():
            local_count[w.lower()] += 1
    return local_count

def merge_dicts(dicts):
    merged = defaultdict(int)
    for d in dicts:
        for k, v in d.items():
            merged[k] += v
    return merged


if __name__ == '__main__':
    words = defaultdict(int)

    with open(sys.argv[1], 'r') as f:
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(count_words, lines) for lines in chunks(f, 100000)]
            for future in as_completed(futures):
                words = merge_dicts([words, future.result()])

    print(words["thee"])
