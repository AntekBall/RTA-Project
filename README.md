# RTA-Project

# Credit Decisions Dashboard

## Project Description

The **Credit Decisions Dashboard** project is a web application designed to visualize and monitor credit decisions made by machine learning models in real time. The input data comes from a Kafka message queue system, where the application listens for messages containing client information and model prediction results.

The goal of the project is to compare two classification models (XGBoost and Random Forest) in the context of accepting or rejecting credit applications, and to present the experiment results through interactive charts on the dashboard.

## Main Features

* **Real-time data streaming** — receiving client data and model decisions from the Kafka broker.
* **ML model predictions** — using preloaded XGBoost and Random Forest models to assess credit risk.
* **Visualization dashboard** — interactive charts displaying:

  * The number of positive and negative decisions broken down by model.
  * Cumulative time trends of decisions.
  * ROC curves for both models.
  * Confusion matrices separated by model.
  * Distribution of client incomes depending on the decision and model.
* **A/B Experiment** — analysis of results from both models as two variants of the experiment.
