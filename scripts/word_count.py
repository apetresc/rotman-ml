from collections import defaultdict
import sys

words = defaultdict(int)

with open(sys.argv[1], 'r') as f:
    for l in f:
        for w in l.split():
            words[w.lower()] += 1

print(words["thee"])
