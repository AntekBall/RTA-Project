import json
import random
from datetime import datetime
from time import sleep
from kafka import KafkaProducer

SERVER = "broker:9092"  
TOPIC = "credit_applications"  


# This has to be changed to create clients based on the distribution of our dataset
def generate_client():
    """Generates synthetic client data for credit evaluation"""
    client_id = f"CL{random.randint(10000, 99999)}"
    current_time = datetime.now()
    
    return {
        "timestamp": str(current_time),
        "client_id": client_id,
        "age": random.randint(18, 70),
        "annual_income": random.randint(20000, 200000),
        "credit_score": random.randint(300, 850),
        "debt_to_income_ratio": round(random.uniform(0.1, 0.8), 2),
        "employment_status": random.choice(["employed", "self-employed", "unemployed"]),
        "loan_amount": random.randint(5000, 50000),
        "loan_purpose": random.choice(["mortgage", "car", "education", "personal"]),
        "default_history": random.randint(0, 3)
    }

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=[SERVER],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    
    try:
        print(f"Producing client data to {TOPIC}. Press Ctrl+C to stop...")
        while True:
            client_data = generate_client()
            producer.send(TOPIC, value=client_data)
            print(f"Sent client: {client_data['client_id']} (Score: {client_data['credit_score']})")
            sleep(random.uniform(0.5, 2.0))  
            
    except KeyboardInterrupt:
        print("\nStopping producer...")
        producer.close()