#!/usr/bin/env python

import sys
import datetime

PICKUP_DATETIME = 1
PAYMENT_TYPE = 9
TIP_AMOUNT = 13

PAYMENTS = {
    '1': 'Credit card',
    '2': 'Cash',
    '3': 'No charge',
    '4': 'Dispute',
    '5': 'Unknown',
    '6': 'Voided trip'
}


def main():
    for line in sys.stdin:
        line = line.strip()
        parts = line.split(',')

        # Attempt to parse the date and filter only for the year 2020 and non-empty payment type
        pickup_datetime = parts[PICKUP_DATETIME]
        payment_type = parts[PAYMENT_TYPE].strip()
        tip_amount = parts[TIP_AMOUNT].strip()

        try:
            dt = datetime.datetime.strptime(pickup_datetime, '%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            continue

        if dt.year != 2020 or payment_type not in PAYMENTS:
            continue

        try:
            tip_amount = float(tip_amount)
        except ValueError:
            continue

        # Output year-month-payment type
        print("{}-{}\t{:.2f}".format(dt.strftime("%Y-%m"), payment_type, tip_amount))


if __name__ == '__main__':
    main()
