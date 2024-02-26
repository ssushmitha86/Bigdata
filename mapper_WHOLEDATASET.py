#!/usr/bin/env python3
import sys
from collections import defaultdict

stop_words = set()

with open("stop_words.txt", "r") as stop_words_file:
    stop_words = set(map(str.strip, stop_words_file.readlines()))

local_counts = defaultdict(int)

for line in sys.stdin:
    words = line.lower().split()
    for word in words:
        if word not in stop_words:
            local_counts[word] += 1

for word, count in local_counts.items():
    print(f"{word}\t{count}")
