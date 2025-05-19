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

# Configuration
KAFKA_BROKER = "broker:9092"
TOPIC = "credit_applications"
DASH_PORT = 4040

class DashboardData:
    def __init__(self):
        self.decisions = []
        self.experiment_results = {
            "A": {"APPROVED": 0, "DENIED": 0},
            "B": {"APPROVED": 0, "DENIED": 0}
        }
        self.lock = threading.Lock()

    def add_decision(self, client_data, decision):
        with self.lock:
            self.decisions.append({
                'client_id': client_data['client_id'],
                'decision': decision,
                'credit_score': client_data['credit_score'],
                'income': client_data['annual_income'],
                'dti': client_data['debt_to_income_ratio'],
                'timestamp': client_data['timestamp'],
                'experiment_group': client_data['experiment_group']
            })
            if client_data['experiment_group'] in self.experiment_results:
                self.experiment_results[client_data['experiment_group']][decision.split()[0]] += 1

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
    consumer = get_kafka_consumer()
    print(f"Connected to Kafka at {KAFKA_BROKER}")
    for message in consumer:
        try:
            client_data = message.value
            
            # Skip message if experiment_group is missing
            if 'experiment_group' not in client_data:
                print(f"Warning: Missing 'experiment_group' for client {client_data['client_id']}")
                continue  # Skip this message

            # Different decision rules based on experiment group
            if client_data['experiment_group'] == "A":  # Control Group
                # Control group rules (A)
                if client_data['credit_score'] >= 650 and client_data['debt_to_income_ratio'] <= 0.4:
                    decision = "APPROVED"
                elif client_data['credit_score'] >= 600 and client_data['annual_income'] > 50000 and client_data['debt_to_income_ratio'] <= 0.5:
                    decision = "APPROVED (MARGINAL)"
                else:
                    decision = "DENIED"
            else:  # Test Group (B)
                # Test group rules (B)
                if client_data['credit_score'] >= 620 and client_data['debt_to_income_ratio'] <= 0.45:
                    decision = "APPROVED"
                elif client_data['credit_score'] >= 580 and client_data['annual_income'] > 45000 and client_data['debt_to_income_ratio'] <= 0.55:
                    decision = "APPROVED (MARGINAL)"
                else:
                    decision = "DENIED"

            # Add the decision to the data store
            data_store.add_decision(client_data, decision)

        except Exception as e:
            print(f"Error processing message: {e}")


# Dash App Setup
app = dash.Dash(__name__)
app.layout = html.Div([
    html.H1("Credit Decisions Dashboard"),

    dcc.Graph(id='live-pie'),

    html.Div([
        dcc.Graph(id='experiment-results'),
        dcc.Graph(id='score-distribution')
    ], style={'display': 'flex'}),

    html.Div([
        dcc.Graph(id='processing-time'),  # Placeholder
        dcc.Graph(id='throughput')        # Placeholder
    ], style={'display': 'flex'}),

    dcc.Interval(id='refresh', interval=2000)
])

@app.callback(
    Output('live-pie', 'figure'),
    Input('refresh', 'n_intervals')
)
def update_pie(_):
    with data_store.lock:
        df = pd.DataFrame(data_store.decisions or [])

    if df.empty:
        return px.pie(
            pd.DataFrame({'decision': [], 'count': []}),
            names='decision',
            title="Waiting for data..."
        )

    # Group the data by decision and experiment group
    return px.pie(
        df,
        names='decision',
        title='Approval Rate by Decision',
        color='decision',
        color_discrete_map={'APPROVED': 'green', 'APPROVED (MARGINAL)': 'yellow', 'DENIED': 'red'},
        facet_col="experiment_group",  # Split the pie chart by experiment group
        facet_col_wrap=2,  # Wrap the groups into two columns for better layout
    )


@app.callback(
    Output('experiment-results', 'figure'),
    Input('refresh', 'n_intervals')
)
def update_experiment_results(_):
    with data_store.lock:
        df = pd.DataFrame(data_store.decisions or [])
        exp_data = data_store.experiment_results

    if df.empty:
        return px.bar(
            pd.DataFrame(columns=["Group", "Decision", "Count"]),
            x="Group", y="Count", color="Decision",
            title="Waiting for experiment data..."
        )

    exp_df = pd.DataFrame([
        {"Group": "A", "Decision": "APPROVED", "Count": exp_data["A"]["APPROVED"]},
        {"Group": "A", "Decision": "DENIED", "Count": exp_data["A"]["DENIED"]},
        {"Group": "B", "Decision": "APPROVED", "Count": exp_data["B"]["APPROVED"]},
        {"Group": "B", "Decision": "DENIED", "Count": exp_data["B"]["DENIED"]}
    ])

    return px.bar(
        exp_df,
        x="Group",
        y="Count",
        color="Decision",
        barmode="group",
        title="A/B Test Results",
        color_discrete_map={"APPROVED": "green", "DENIED": "red"}
    )

@app.callback(
    Output('score-distribution', 'figure'),
    Input('refresh', 'n_intervals')
)
def update_score_distribution(_):
    with data_store.lock:
        df = pd.DataFrame(data_store.decisions or [])

    if df.empty:
        return px.histogram(
            pd.DataFrame({'credit_score': [], 'decision': []}),
            x="credit_score",
            color="decision",
            nbins=20,
            title="Waiting for data..."
        )

    return px.histogram(
        df,
        x="credit_score",
        color="decision",
        nbins=20,
        title="Credit Score Distribution by Decision",
        color_discrete_map={"APPROVED": "green", "DENIED": "red"}
    )

if __name__ == '__main__':
    threading.Thread(target=consume_messages, daemon=True).start()

    local_ip = socket.gethostbyname(socket.gethostname())
    print(f"\n=== Dashboard Access ===")
    print(f"From host machine: http://localhost:{DASH_PORT}")
    # print(f"From network: http://{local_ip}:{DASH_PORT}\n")

    app.run(host='0.0.0.0', port=DASH_PORT, debug=False)
