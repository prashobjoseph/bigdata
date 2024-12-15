#!/usr/bin/env python
import sys

# Reducer function for CSV processing (aggregation)
def reducer():
    current_key = None
    current_sum = 0.0

    for line in sys.stdin:
        line = line.strip()
        key, value = line.split('\t', 1)

        try:
            value = float(value)
        except ValueError:
            continue  # Skip lines where value can't be converted to float

        if current_key == key:
            current_sum += value
        else:
            if current_key:
                # Emit the previous key and its summed value
                print(f"{current_key}\t{current_sum}")
            current_key = key
            current_sum = value

    # Emit the last key and its summed value
    if current_key == key:
        print(f"{current_key}\t{current_sum}")

if __name__ == "__main__":
    reducer()