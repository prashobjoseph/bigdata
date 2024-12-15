#!/usr/bin/env python
import sys

# Reducer function to aggregate word counts
def reducer():
    current_word = None
    current_count = 0

    for line in sys.stdin:
        # Strip leading/trailing spaces and split the line into word and count
        line = line.strip()
        word, count = line.split('\t', 1)

        try:
            count = int(count)
        except ValueError:
            # Ignore lines that can't be converted to integers
            continue

        if current_word == word:
            current_count += count
        else:
            if current_word:
                # Output the previous word and its total count
                print(f"{current_word}\t{current_count}")
            current_word = word
            current_count = count

    # Output the last word and its total count
    if current_word == word:
        print(f"{current_word}\t{current_count}")

if __name__ == "__main__":
    reducer()