#!/usr/bin/env python
import sys
import csv

# Mapper function for CSV processing
def mapper():
    reader = csv.reader(sys.stdin)
    # Skip the header row
    next(reader)

    for row in reader:
        if len(row) < 4:
            continue  # Skip malformed rows

        name = row[1].strip()  # Name column
        category = row[2].strip()  # Category column
        amount = row[3].strip()  # Amount column

        try:
            amount = float(amount)
        except ValueError:
            continue  # Skip rows where amount is not a valid number
        
        # Emitting name or category as key, and amount as value
        # You can change 'name' or 'category' as needed for your use case
        print(f"{category}\t{amount}")

if __name__ == "__main__":
    mapper()