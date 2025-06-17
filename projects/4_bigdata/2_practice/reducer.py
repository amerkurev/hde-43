#!/usr/bin/env python

import sys
from collections import defaultdict


PAYMENTS = {
    '1': 'Credit card',
    '2': 'Cash',
    '3': 'No charge',
    '4': 'Dispute',
    '5': 'Unknown',
    '6': 'Voided trip'
}


def main():
    totals = defaultdict(float)
    counts = defaultdict(int)

    for line in sys.stdin:
        line = line.strip()
        key, tip_amount_str = line.split('\t', 1)
        tip_amount = float(tip_amount_str)  # mapper already guarantees that tip_amount_str is a number

        totals[key] += tip_amount
        counts[key] += 1

    # Output the final CSV
    for key in sorted(totals):
        month, payment_code = key.rsplit('-', 1)
        payment_name = PAYMENTS[payment_code]
        average = totals[key] / counts[key]
        print("{},{},{:.3f}".format(month, payment_name, average))


if __name__ == '__main__':
    main()
