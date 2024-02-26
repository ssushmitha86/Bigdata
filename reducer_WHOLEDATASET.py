#!/usr/bin/env python3
import sys
from collections import defaultdict

counts = defaultdict(int)

for line in sys.stdin:
    word, count = line.strip().split('\t')
    counts[word] += int(count)  # Cast count to integer before adding

# Get the 100 most common words
top_100_words = sorted(counts.items(), key=lambda item: item[1], reverse=True)[:100]

for word, count in top_100_words:
    print(f"{word}\t{count}")
