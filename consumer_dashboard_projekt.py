import json
import threading
import time
from datetime import datetime
from kafka import KafkaConsumer

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import socket

import json
import pickle
import joblib

from sklearn.metrics import roc_curve, confusion_matrix

# Configuration
KAFKA_BROKER = "broker:9092"
TOPIC = "credit_applications"
DASH_PORT = 4040


############################ DECISION-MAKING MODELS ############################

## Data columns
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
    ## Loading decision-making models
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
            if prediction == 0:
                decision = "APPROVED"
                confidence = prediction_proba[0]
            else:
                decision = "DENIED"
                confidence = prediction_proba[1]
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


############################ REAL-TIME DATA FOR DASHBOARD  ############################
class DashboardData:
    def __init__(self):
        self.decisions = []
        self.experiment_results = {
            "A": {"APPROVED": 0, "DENIED": 0},  # XGBoost
            "B": {"APPROVED": 0, "DENIED": 0}   # random_forest
        }
        self.lock = threading.Lock()

    def add_decision(self, client_data, decision, actual_decision, model):
        with self.lock:
            experiment_group = None
            if model=='XGBoost':
                experiment_group = "A"
            elif model=='random_forest':
                experiment_group = "B"
            
            self.decisions.append({
                'client_id': client_data['client_id'],
                'decision': decision,
                'actual_decision': actual_decision,
                'income': client_data['StatedMonthlyIncome'],
                'dti': client_data['DebtToIncomeRatio'],
                'timestamp': client_data['timestamp'],
                'experiment_group': experiment_group
            })
            if experiment_group in self.experiment_results:
                self.experiment_results[experiment_group][decision.split()[0]] += 1

data_store = DashboardData()

def get_kafka_consumer():
    max_retries = 5
    for attempt in range(max_retries):
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='dashboard-group'
            )
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            print(f"Kafka connection failed (attempt {attempt + 1}), retrying in 5s...")
            time.sleep(5)

def consume_messages():
    models = load_models()
    if models is None:
        print("Cannot proceed without models. Exiting...")
        exit(1)
    
    consumer = get_kafka_consumer()
    print(f"Connected to Kafka at {KAFKA_BROKER}")
    for message in consumer:
        try:
            client_data = message.value
            
            # Different decision rules based on experiment group
            results = make_prediction(client_data, models)

            # Add the decision to the data store
            actual_target = 'APPROVED' if results['actual_target'] else 'DENIED'
            data_store.add_decision(client_data, results['XGBoost']['decision'], actual_target, "XGBoost")
            data_store.add_decision(client_data, results['random_forest']['decision'], actual_target, "random_forest")

        except Exception as e:
            print(f"Error processing message: {e}")


############################ DASH APP SETUP  ############################

colors = {
    'background': '#242424',
    'text': '#7FDBFF',
    'model_A': '#7FDBFF',
    'model_B': '#98FBCB'
}

app = dash.Dash(__name__)
app.layout = html.Div(
    style={
        'backgroundColor': colors['background'],
        'fontFamily': 'Arial, sans-serif',
        'padding': '20px'
    },
    children=[
        html.H1(
            "Credit Decisions Dashboard",
            style={
                "font-family": "Arial, sans-serif",
                "font-size": "36px",
                "text-align": "center",
                "color": colors['text']
            }
        ),

        dcc.Graph(id='experiment-results'),
        dcc.Graph(id='approval-by-time'),
        dcc.Graph(id='confusion-matrix'),
        dcc.Graph(id='income-distribution'),

        dcc.Interval(id='refresh', interval=2000)
    ]
)


# @app.callback(
#     Output('live-pie', 'figure'),
#     Input('refresh', 'n_intervals')
# )
# def update_pie(_):
#     with data_store.lock:
#         df = pd.DataFrame(data_store.decisions or [])

#     if df.empty:
#         return px.pie(
#             pd.DataFrame({'decision': [], 'count': []}),
#             names='decision',
#             title="Waiting for data..."
#         )

#     # Group the data by decision and experiment group
#     return px.pie(
#         df,
#         names='decision',
#         title='Approval Rate by Decision',
#         color='decision',
#         color_discrete_map={'APPROVED': 'green', 'APPROVED (MARGINAL)': 'yellow', 'DENIED': 'red'},
#         facet_col="experiment_group",  # Split the pie chart by experiment group
#         facet_col_wrap=2,  # Wrap the groups into two columns for better layout
#     )

@app.callback(
    Output('experiment-results', 'figure'),
    Input('refresh', 'n_intervals')
)
def update_experiment_results(_):
    with data_store.lock:
        df = pd.DataFrame(data_store.decisions or [])
        exp_data = data_store.experiment_results

    if df.empty:
        fig=px.bar(
            pd.DataFrame(columns=["Group", "Decision", "Count"]),
            x="Group",
            y="Count",
            color="Decision",
            barmode="stack",
            title="Waiting for experiment data...",
            labels={'Group': 'Model Variant', 'Count': '', 'decision': 'Decision'}
            )
        fig.update_layout(
            plot_bgcolor=colors['background'],
            paper_bgcolor=colors['background'],
            font_color=colors['text']
        )
        return fig

    exp_df = pd.DataFrame([
        {"Group": "A", "Decision": "APPROVED", "Count": exp_data["A"]["APPROVED"]},
        {"Group": "A", "Decision": "DENIED", "Count": exp_data["A"]["DENIED"]},
        {"Group": "B", "Decision": "APPROVED", "Count": exp_data["B"]["APPROVED"]},
        {"Group": "B", "Decision": "DENIED", "Count": exp_data["B"]["DENIED"]}
    ])

    fig = px.bar(
        exp_df,
        x="Group",
        y="Count",
        color="Decision",
        barmode="stack",
        title="Approval and Denial Outcomes by Model Variant",
        labels={'Group': 'Model Variant', 'Count': '', 'decision': 'Decision'},
        color_discrete_map={"APPROVED": colors['model_A'], "DENIED": colors['model_B']}
    )
    fig.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font_color=colors['text']
    )
    return fig


@app.callback(
    Output('approval-by-time', 'figure'),
    Input('refresh', 'n_intervals')
)
def update_approval_by_time(_):
    with data_store.lock:
        df = pd.DataFrame(data_store.decisions or [])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        counts = df.groupby(['timestamp', 'decision']).size().unstack(fill_value=0)
        cum_counts = counts.cumsum()
        cum_counts_long = cum_counts.reset_index().melt(id_vars='timestamp', var_name='decision', value_name='cum_count')

    if df.empty:
        fig=px.line(
            pd.DataFrame(columns=["timestamp", "cum_count", "decision"]),
            x='timestamp',
            y='cum_count',
            color='decision',
            markers=False,  # punkty na linii, opcjonalnie
            labels={'timestamp': 'Czas', 'cum_count': 'Skumulowana liczba decyzji', 'decision': 'Decyzja'},
            title="Waiting for data..."
        )
        fig.update_layout(
            plot_bgcolor=colors['background'],
            paper_bgcolor=colors['background'],
            font_color=colors['text']
        )
        return fig

    color_map = {
        'APPROVED': colors['model_B'],
        'DENIED': colors['model_A']
    }

    fig=px.line(
        cum_counts_long,
        x='timestamp',
        y='cum_count',
        color='decision',
        markers=True,  # punkty na linii, opcjonalnie
        labels={'timestamp': 'Date & time', 'cum_count': 'Cumulative Number of Decisions', 'decision': 'Decision'},
        color_discrete_map=color_map,
        title='Cumulative Number of Decisions Over Time by Outcome'
    )
    fig.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font_color=colors['text']
    )
    return fig


# @app.callback(
#     Output('roc', 'figure'),
#     Input('refresh', 'n_intervals')
# )
# def update_roc(_):
#     with data_store.lock:
#         df = pd.DataFrame(data_store.decisions or [])
#         df['y_true'] = df['actual_decision'].map({'APPROVED': 1, 'DENIED': 0})
#         df['y_pred'] = df['decision'].map({'APPROVED': 1, 'DENIED': 0})
#         roc_rows = []

#         for group in ['A', 'B']:
#             df_group = df[df['experiment_group'] == group]
#             fpr, tpr, _ = roc_curve(df_group['y_true'], df_group['y_pred'])
        
#             for fp, tp in zip(fpr, tpr):
#                 roc_rows.append({'fpr': fp, 'tpr': tp, 'experiment_group': group})

#         roc_df = pd.DataFrame(roc_rows)
    
#     if df.empty:
#         fig=px.line(
#             roc_df,
#             x='fpr',
#             color='experiment_group',
#             title="Waiting for data...",
#             labels={'fpr': 'False Positive Rate (FPR)', 'tpr': 'True Positive Rate (TPR)', 'experiment_group': 'Grupa'}
#         )
#         fig.update_layout(
#             plot_bgcolor=colors['background'],
#             paper_bgcolor=colors['background'],
#             font_color=colors['text']
#         )
#         return fig

#     fig=px.line(
#         roc_df,
#         x='fpr',
#         y='tpr',
#         color='experiment_group',
#         title='Krzywa ROC dla grup A i B',
#         labels={'fpr': 'False Positive Rate (FPR)', 'tpr': 'True Positive Rate (TPR)', 'experiment_group': 'Grupa'}
#     )
#     fig.update_layout(
#         plot_bgcolor=colors['background'],
#         paper_bgcolor=colors['background'],
#         font_color=colors['text']
#     )
#     return fig


@app.callback(
    Output('confusion-matrix', 'figure'),
    Input('refresh', 'n_intervals')
)
def update_confusion_matrix(_):
    with data_store.lock:
        df = pd.DataFrame(data_store.decisions or [])

    if df.empty:
        empty_df = pd.DataFrame(columns=['Actual', 'Predicted', 'Count', 'experiment_group'])
        fig=px.imshow(
            empty_df.pivot(index='Actual', columns='Predicted', values='Count'),
            title='Waiting for data...'
        )
        fig.update_layout(
            plot_bgcolor=colors['background'],
            paper_bgcolor=colors['background'],
            font_color=colors['text']
        )
        return fig

    # Zakodowanie binarne
    df['y_true'] = df['actual_decision'].map({'APPROVED': 1, 'DENIED': 0})
    df['y_pred'] = df['decision'].map({'APPROVED': 1, 'DENIED': 0})

    def prepare_cm_df(group_label):
        group_df = df[df['experiment_group'] == group_label]
        cm = confusion_matrix(group_df['y_true'], group_df['y_pred'], labels=[1, 0])
        cm_df = pd.DataFrame(
            cm,
            # index=['Actual APPROVED', 'Actual DENIED'],
            index=['Actual DEFAULT', 'Actual NON-DEFAULT'],
            columns=['Pred APPROVED', 'Pred DENIED']
        ).reset_index().melt(id_vars='index', var_name='Predicted', value_name='Count')
        cm_df.rename(columns={'index': 'Actual'}, inplace=True)
        cm_df['experiment_group'] = group_label
        return cm_df

    cm_a = prepare_cm_df('A')
    cm_b = prepare_cm_df('B')
    cm_all = pd.concat([cm_a, cm_b], ignore_index=True)

    # Wykres heatmapy z podziałem na grupy
    fig = px.density_heatmap(
        cm_all,
        x='Predicted',
        y='Actual',
        z='Count',
        facet_col='experiment_group',
        facet_col_wrap=2,
        color_continuous_scale='mint',
        text_auto=True,
        title='Confusion Matrix per Experiment Group',
        labels={'Predicted': '', 'Actual': ''}
    )

    for annotation in fig.layout.annotations:
        if annotation.text == 'experiment_group=A':
            annotation.text = 'Model A'
        elif annotation.text == 'experiment_group=B':
            annotation.text = 'Model B'
    fig.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font_color=colors['text']
    )
    return fig

@app.callback(
    Output('income-distribution', 'figure'),
    Input('refresh', 'n_intervals')
)
def update_income_distribution(_):
    with data_store.lock:
        df = pd.DataFrame(data_store.decisions or [])

    if df.empty:
        fig=px.histogram(
            pd.DataFrame(columns=['income', 'decision', 'experiment_group']),
            x="income", color="decision",
            title="Waiting for data..."
        )
        fig.update_layout(
            plot_bgcolor=colors['background'],
            paper_bgcolor=colors['background'],
            font_color=colors['text']
        )
        return fig

    fig=px.histogram(
        df,
        x='income',
        color='decision',
        facet_col='experiment_group',
        nbins=30,
        barmode='overlay',
        opacity=0.6,
        color_discrete_map={"APPROVED": colors['model_A'], "DENIED": colors['model_B']},
        title="Income Distribution by Decision and Experiment Group",
        labels={'income': 'Income', 'decision': 'Decision'}
    )

    fig.update_yaxes(title_text="")

    for annotation in fig.layout.annotations:
        if annotation.text == 'experiment_group=A':
            annotation.text = 'Model A'
        elif annotation.text == 'experiment_group=B':
            annotation.text = 'Model B'
    fig.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font_color=colors['text']
    )
    return fig


if __name__ == '__main__':
    threading.Thread(target=consume_messages, daemon=True).start()

    print(px.colors.sequential.Mint)

    local_ip = socket.gethostbyname(socket.gethostname())
    print(f"\n=== Dashboard Access ===")
    print(f"From host machine: http://localhost:{DASH_PORT}")
    # print(f"From network: http://{local_ip}:{DASH_PORT}\n")

    app.run(host='0.0.0.0', port=DASH_PORT, debug=False)
