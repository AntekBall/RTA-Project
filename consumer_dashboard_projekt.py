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
        self.lock = threading.Lock()

    def add_decision(self, client_data, decision):
        with self.lock:
            self.decisions.append({
                'client_id': client_data['client_id'],
                'decision': decision,
                'credit_score': client_data['credit_score'],
                'income': client_data['annual_income'],
                'dti': client_data['debt_to_income_ratio'],
                'timestamp': client_data['timestamp']
            })

data_store = DashboardData()

def get_kafka_consumer():
    """Create Kafka consumer with retry logic."""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest',
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
        client_data = message.value
        decision = (
            "APPROVED" if client_data['credit_score'] >= 650
            and client_data['debt_to_income_ratio'] <= 0.4
            else "DENIED"
        )
        data_store.add_decision(client_data, decision)

# Dash App Setup
app = dash.Dash(__name__)
app.layout = html.Div([
    html.H1("Credit Decisions Dashboard"),
    dcc.Graph(id='live-pie'),
    dcc.Interval(id='refresh', interval=2000)
])

@app.callback(
    Output('live-pie', 'figure'),
    Input('refresh', 'n_intervals')
)
def update_pie(_):
    with data_store.lock:
        df = pd.DataFrame(data_store.decisions)

    if df.empty:
        return px.pie(title="Waiting for data...")

    return px.pie(
        df,
        names='decision',
        title='Approval Rate',
        color='decision',
        color_discrete_map={'APPROVED': 'green', 'DENIED': 'red'}
    )

if __name__ == '__main__':
    # Start Kafka consumer in background thread
    threading.Thread(target=consume_messages, daemon=True).start()

    # Show access info
    local_ip = socket.gethostbyname(socket.gethostname())
    print(f"\n=== Dashboard Access ===")
    print(f"From host machine: http://localhost:{DASH_PORT}")
    print(f"From network: http://{local_ip}:{DASH_PORT}\n")

    app.run(host='0.0.0.0', port=DASH_PORT, debug=False)
