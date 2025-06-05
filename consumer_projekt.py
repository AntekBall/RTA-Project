import json
import pickle
import pandas as pd
from kafka import KafkaConsumer

SERVER = "broker:9092"  
TOPIC = "credit_applications"  

def load_models():
    """Load trained models"""
    models = {}
    
    try:
        with open('./data/logreg_woe.pkl', 'rb') as f:
            models['logreg_woe'] = pickle.load(f)
        print("Loaded logreg_woe.pkl model")
        
        with open('./data/random_forest.pkl', 'rb') as f:
            models['random_forest'] = pickle.load(f)
        print("Loaded random_forest.pkl model")
        
    except FileNotFoundError as e:
        print(f"Error loading model: {e}")
        return None
    
    return models

def prepare_features(client_data):
    """Prepare features for model prediction"""
    feature_data = client_data.copy()
    
    non_feature_cols = ['timestamp', 'client_id', 'target']
    for col in non_feature_cols:
        if col in feature_data:
            del feature_data[col]
    
    df = pd.DataFrame([feature_data])
    
    return df

def make_prediction(client_data, models):
    """Make prediction using both loaded models"""
    results = {}
    
    try:
        features = prepare_features(client_data)
        
        for model_name, model in models.items():
            prediction = model.predict(features)[0]
            prediction_proba = model.predict_proba(features)[0]
            
            if prediction == 1:
                decision = "APPROVED"
                confidence = prediction_proba[1]
            else:
                decision = "DENIED"
                confidence = prediction_proba[0]
            
            results[model_name] = {
                "decision": decision,
                "confidence": confidence,
                "prediction_proba": prediction_proba.tolist()
            }
        
        return results
        
    except Exception as e:
        print(f"Error making prediction: {e}")
        return {
            "error": str(e)
        }

def display_client_info(client_data, results):
    """Display client information and prediction results"""
    print("\n" + "="*60)
    print(f"Client ID: {client_data['client_id']}")
    print(f"Timestamp: {client_data['timestamp']}")
    
    print("\nClient Features:")
    for key, value in client_data.items():
        if key not in ['timestamp', 'client_id', 'target']:
            if value is not None:
                if isinstance(value, float):
                    print(f"- {key}: {value:.4f}")
                else:
                    print(f"- {key}: {value}")
    
    if 'error' in results:
        print(f"\nERROR: {results['error']}")
    else:
        print(f"\nPREDICTION RESULTS:")
        for model_name, result in results.items():
            print(f"\n{model_name.upper()}:")
            print(f"  - Decision: {result['decision']}")
            print(f"  - Confidence: {result['confidence']:.4f}")
            print(f"  - Probability [Deny, Approve]: {result['prediction_proba']}")
    
    print("="*60 + "\n")

if __name__ == "__main__":
    models = load_models()
    if models is None:
        print("Cannot proceed without models. Exiting...")
        exit(1)
    
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[SERVER],
        auto_offset_reset='latest',  
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print(f"Listening for messages on {TOPIC}. Press Ctrl+C to stop...")
    print(f"Loaded models: {list(models.keys())}")
    
    try:
        for message in consumer:
            client_data = message.value
            results = make_prediction(client_data, models)
            display_client_info(client_data, results)
            
    except KeyboardInterrupt:
        print("\nStopping consumer...")
        consumer.close()