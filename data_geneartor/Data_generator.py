import boto3
import json
import uuid
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
s3 = boto3.client('s3', region_name='us-east-1')

BUCKET = 'fraud-detection-raw-zone'
MERCHANTS = ['Amazon', 'Netflix', 'Uber', 'Swiggy', 'Zomato', 'Flipkart', 'Apple', 'Steam']
COUNTRIES = ['US', 'IN', 'SG', 'LU', 'CH', 'NG', 'RU', 'BR']  # includes high-risk

def generate_transaction(user_id: str, inject_fraud: bool = False) -> dict:
    base_country = random.choice(['US', 'IN', 'SG'])

    if inject_fraud:
        # Fraud patterns: geo anomaly, high amount, rapid succession
        txn_country = random.choice(['NG', 'RU', 'BR'])  # geo anomaly
        amount = round(random.uniform(5000, 50000), 2)    # unusually high
    else:
        txn_country = base_country
        amount = round(random.uniform(5, 500), 2)

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "amount": amount,
        "currency": "USD",
        "merchant": random.choice(MERCHANTS),
        "merchant_category": fake.job(),
        "transaction_country": txn_country,
        "user_home_country": base_country,
        "card_last4": fake.credit_card_number()[-4:],
        "ip_address": fake.ipv4(),
        "device_id": fake.uuid4(),
        "is_fraud_label": inject_fraud  # Ground truth for evaluation
    }

def generate_batch(num_transactions: int = 10000, fraud_rate: float = 0.02) -> list:
    """Generate a batch of transactions with ~2% fraud rate"""
    users = [str(uuid.uuid4()) for _ in range(500)]  # 500 unique users
    transactions = []

    for _ in range(num_transactions):
        user_id = random.choice(users)
        inject_fraud = random.random() < fraud_rate
        transactions.append(generate_transaction(user_id, inject_fraud))

    print(f"Generated {num_transactions} transactions | Fraud: {int(num_transactions * fraud_rate)}")
    return transactions

def upload_to_s3(transactions: list, batch_id: str):
    """Upload as newline-delimited JSON to S3 partitioned by date"""
    date_prefix = datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
    key = f"raw/transactions/{date_prefix}/batch_{batch_id}.json"

    payload = "\n".join(json.dumps(txn) for txn in transactions)

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=payload.encode('utf-8'),
        ContentType='application/json'
    )
    print(f"Uploaded {len(transactions)} records → s3://{BUCKET}/{key}")

if __name__ == "__main__":
    batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    transactions = generate_batch(num_transactions=50000, fraud_rate=0.02)
    upload_to_s3(transactions, batch_id)