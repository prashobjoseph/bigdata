#!/usr/bin/env python
import sys

# Mapper function to read input lines, split into words, and emit word counts
def mapper():
    for line in sys.stdin:
        # Strip leading/trailing spaces and split the line into words
        line = line.strip()
        words = line.split()
        
        for word in words:
            # Output the word with count 1
            print(f"{word.lower()}\t1")

if __name__ == "__main__":
    mapper()