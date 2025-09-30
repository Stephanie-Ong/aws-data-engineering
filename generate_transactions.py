#!/usr/bin/env python3

import random
from datetime import datetime, timedelta

# Set seed for reproducible results
random.seed(42)

# Generate Transaction Data
transactions_data = []
transaction_id = 1
base_date = datetime(2024, 1, 1)

for cust_id in [f'CUST{str(i).zfill(3)}' for i in range(1, 18)]:
    # Each customer gets 5-10 transactions
    num_txns = random.randint(5, 10)
    for _ in range(num_txns):
        amount = round(random.uniform(10.0, 5000.0), 2)
        days_offset = random.randint(0, 180)
        txn_date = base_date + timedelta(days=days_offset)
        txn_type = random.choice(['Purchase', 'Withdrawal', 'Transfer', 'Deposit'])
        
        transactions_data.append((
            f'TXN{str(transaction_id).zfill(6)}',
            cust_id,
            amount,
            txn_date.strftime('%Y-%m-%d'),
            txn_type
        ))
        transaction_id += 1

# Output CSV format
print('transaction_id,customer_id,amount,transaction_date,transaction_type')
for txn in transactions_data:
    print(f'{txn[0]},{txn[1]},{txn[2]},{txn[3]},{txn[4]}')