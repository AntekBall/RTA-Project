import json
import random
import pandas as pd
from datetime import datetime
from time import sleep
from kafka import KafkaProducer

SERVER = "broker:9092"  
TOPIC = "credit_applications"  

def load_test_data():
    """Load test data from CSV files"""
    try:
        X_test = pd.read_csv('./data/X_test.csv')
        y_test = pd.read_csv('./data/y_test.csv')
        
        # Combine X_test and y_test
        test_data = X_test.copy()
        test_data['target'] = y_test.iloc[:, 0] if len(y_test.columns) == 1 else y_test['target']
        
        print(f"Loaded {len(test_data)} test records")
        print(f"Columns: {list(test_data.columns)}")
        return test_data
    except FileNotFoundError as e:
        print(f"Error loading test data: {e}")
        print("Make sure X_test.csv and y_test.csv are in the current directory")
        return None

def generate_client_from_data(test_data):
    """Generate client data from real test dataset"""
    random_idx = random.randint(0, len(test_data) - 1)
    client_row = test_data.iloc[random_idx]
    
    client_id = f"CL{random.randint(10000, 99999)}"
    current_time = datetime.now()
    
    client_data = client_row.to_dict()
    
    for key, value in client_data.items():
        if pd.isna(value):
            client_data[key] = None
    
    # Add metadata
    client_data.update({
        "timestamp": str(current_time),
        "client_id": client_id
    })
    
    return client_data

if __name__ == "__main__":
    test_data = load_test_data()
    if test_data is None:
        print("Cannot proceed without test data. Exiting...")
        exit(1)
    
    producer = KafkaProducer(
        bootstrap_servers=[SERVER],
        value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
    )
    
    try:
        print(f"Producing client data to {TOPIC}. Press Ctrl+C to stop...")
        while True:
            client_data = generate_client_from_data(test_data)
            producer.send(TOPIC, value=client_data)
            
            print(f"Sent client: {client_data['client_id']}")
            
            sleep(random.uniform(0.5, 2.0))  
            
    except KeyboardInterrupt:
        print("\nStopping producer...")
        producer.close()