import json
import pickle
import pandas as pd
import joblib
from kafka import KafkaConsumer

SERVER = "broker:9092"
TOPIC = "credit_applications"

categorical_cols = ['EmploymentStatus', 'IncomeRange', 'BorrowerState']

numeric_cols = [
    'DelinquenciesLast7Years', 'CurrentlyInGroup', 'Term', 'EmploymentStatusDuration',
    'BankcardUtilization', 'TotalTrades', 'InquiriesLast6Months', 'AmountDelinquent',
    'StatedMonthlyIncome', 'CurrentDelinquencies', 'PublicRecordsLast10Years',
    'IncomeVerifiable', 'TotalInquiries', 'IsBorrowerHomeowner',
    'TradesNeverDelinquent (percentage)', 'BorrowerRate', 'LoanOriginalAmount',
    'BorrowerAPR', 'ListingCategory (numeric)', 'LenderYield', 'MonthlyLoanPayment',
    'OpenRevolvingAccounts', 'OpenCreditLines', 'AvailableBankcardCredit',
    'TradesOpenedLast6Months', 'Investors', 'DebtToIncomeRatio', 'RevolvingCreditBalance',
    'TotalCreditLinespast7years', 'OpenRevolvingMonthlyPayment'
]

MODEL_COLUMNS = [
    'DelinquenciesLast7Years', 'CurrentlyInGroup', 'Term', 'EmploymentStatusDuration',
    'BankcardUtilization', 'TotalTrades', 'InquiriesLast6Months', 'AmountDelinquent',
    'StatedMonthlyIncome', 'CurrentDelinquencies', 'PublicRecordsLast10Years',
    'IncomeVerifiable', 'TotalInquiries', 'IsBorrowerHomeowner',
    'TradesNeverDelinquent (percentage)', 'BorrowerRate', 'LoanOriginalAmount',
    'BorrowerAPR', 'ListingCategory (numeric)', 'LenderYield', 'MonthlyLoanPayment',
    'OpenRevolvingAccounts', 'OpenCreditLines', 'AvailableBankcardCredit',
    'TradesOpenedLast6Months', 'Investors', 'DebtToIncomeRatio', 'RevolvingCreditBalance',
    'TotalCreditLinespast7years', 'OpenRevolvingMonthlyPayment',
    'EmploymentStatus_Full-time', 'EmploymentStatus_Not available', 'EmploymentStatus_Not employed',
    'EmploymentStatus_Other', 'EmploymentStatus_Part-time', 'EmploymentStatus_Retired',
    'EmploymentStatus_Self-employed',
    'IncomeRange_$1-24,999', 'IncomeRange_$100,000+', 'IncomeRange_$25,000-49,999',
    'IncomeRange_$50,000-74,999', 'IncomeRange_$75,000-99,999', 'IncomeRange_Not displayed',
    'IncomeRange_Not employed',
    'BorrowerState_AL', 'BorrowerState_AR', 'BorrowerState_AZ', 'BorrowerState_CA',
    'BorrowerState_CO', 'BorrowerState_CT', 'BorrowerState_DC', 'BorrowerState_DE',
    'BorrowerState_FL', 'BorrowerState_GA', 'BorrowerState_HI', 'BorrowerState_IA',
    'BorrowerState_ID', 'BorrowerState_IL', 'BorrowerState_IN', 'BorrowerState_KS',
    'BorrowerState_KY', 'BorrowerState_LA', 'BorrowerState_MA', 'BorrowerState_MD',
    'BorrowerState_ME', 'BorrowerState_MI', 'BorrowerState_MN', 'BorrowerState_MO',
    'BorrowerState_MS', 'BorrowerState_MT', 'BorrowerState_NC', 'BorrowerState_ND',
    'BorrowerState_NE', 'BorrowerState_NH', 'BorrowerState_NJ', 'BorrowerState_NM',
    'BorrowerState_NV', 'BorrowerState_NY', 'BorrowerState_OH', 'BorrowerState_OK',
    'BorrowerState_OR', 'BorrowerState_PA', 'BorrowerState_RI', 'BorrowerState_SC',
    'BorrowerState_SD', 'BorrowerState_TN', 'BorrowerState_TX', 'BorrowerState_UT',
    'BorrowerState_VA', 'BorrowerState_VT', 'BorrowerState_WA', 'BorrowerState_WI',
    'BorrowerState_WV', 'BorrowerState_WY'
]

def load_models():
    models = {}
    try:
        with open('./data/XGB.pkl', 'rb') as f:
            models['XGBoost'] = pickle.load(f)
            print(f"XGB.pkl loaded: {type(models['XGBoost'])}")
        models['random_forest'] = joblib.load('./data/random_forest.pkl')
        print(f"random_forest.pkl loaded: {type(models['random_forest'])}")
    except FileNotFoundError as e:
        print(f"Error loading model: {e}")
        return None
    return models


def prepare_features(client_data):
    data = client_data.copy()
    for key in ['timestamp', 'client_id', 'target']:
        data.pop(key, None)
    
    df = pd.DataFrame([data])
    dummies = pd.get_dummies(df[categorical_cols], drop_first=True)
    df_numeric = df[numeric_cols]
    df_final = pd.concat([df_numeric, dummies], axis=1)
    df_final = df_final.reindex(columns=MODEL_COLUMNS, fill_value=0)
    return df_final

def make_prediction(client_data, models):
    results = {}
    try:
        target = client_data.get('target')
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
        results["actual_target"] = target
        return results
    except Exception as e:
        print(f"Error making prediction: {e}")
        return {"error": str(e)}

def display_client_info(client_data, results):
    print("\n" + "="*60)
    print(f"Client ID: {client_data.get('client_id', 'N/A')}")
    print(f"Timestamp: {client_data.get('timestamp', 'N/A')}")
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
        actual_target = results.get("actual_target", "N/A")
        for model_name, result in results.items():
            if model_name != "actual_target":
                print(f"\n{model_name.upper()}:")
                print(f"  - Model decision: {result['decision']}")
                print(f"  - Actual target: {'APPROVED' if actual_target else 'DENIED'}")
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
