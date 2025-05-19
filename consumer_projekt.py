import json
from kafka import KafkaConsumer

SERVER = "broker:9092"  
TOPIC = "credit_applications"  

# Also we have to load model here

def make_decision(client_data):
    """Decision logic with A/B testing"""
    credit_score = client_data['credit_score']
    income = client_data['annual_income']
    dti = client_data['debt_to_income_ratio']
    
    # Control group (A) - original rules
    if client_data['experiment_group'] == "A":
        if credit_score >= 650 and dti <= 0.4:
            return "APPROVED"
        elif credit_score >= 600 and income > 50000 and dti <= 0.5:
            return "APPROVED (MARGINAL)"
        else:
            return "DENIED"
    
    # Test group (B) - relaxed rules for testing
    else:
        if credit_score >= 620 and dti <= 0.45:
            return "APPROVED"
        elif credit_score >= 580 and income > 45000 and dti <= 0.55:
            return "APPROVED (MARGINAL)"
        else:
            return "DENIED"

if __name__ == "__main__":
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[SERVER],
        auto_offset_reset='latest',  
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print(f"Listening for messages on {TOPIC}. Press Ctrl+C to stop...")
    try:
        for message in consumer:
            client = message.value
            decision = make_decision(client)
            
            print("\n" + "="*50)
            print(f"Received application from {client['client_id']}:")
            print(f"- Credit Score: {client['credit_score']}")
            print(f"- Annual Income: ${client['annual_income']:,}")
            print(f"- Debt-to-Income: {client['debt_to_income_ratio']*100:.0f}%")
            print(f"\nDECISION: {decision}")
            print("="*50 + "\n")
            
    except KeyboardInterrupt:
        print("\nStopping consumer...")
        consumer.close()