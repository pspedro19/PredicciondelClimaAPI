import datetime
from airflow.decorators import dag, task
import pandas as pd
import mlflow
from mlflow.tracking import MlflowClient
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import f1_score
import awswrangler as wr
from imblearn.over_sampling import SMOTE
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from imblearn.pipeline import Pipeline as ImbPipeline

# Set MLflow tracking URI to use the MLflow service managed by Docker
mlflow.set_tracking_uri("http://mlflow:5000")

# Function to create or get MLflow experiment
def get_or_create_experiment(experiment_name):
    client = MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment:
        return experiment.experiment_id
    else:
        return mlflow.create_experiment(experiment_name)

# Define the DAG and its schedule
default_args = {
    'owner': 'your_owner',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

@dag(dag_id='data_science_pipeline', default_args=default_args, schedule_interval=None, tags=['data-science'])
def data_science_pipeline():
    @task
    def load_data():
        # Load data from S3 bucket using MinIO
        return wr.s3.read_csv("s3://data/data_info/weatherAUS.csv", sep=';', 
                              boto3_session=wr.Session(aws_access_key_id='minio', 
                                                       aws_secret_access_key='minio123',
                                                       endpoint_url='http://minio:9000'))

    @task
    def preprocess_data(df):
        # Data preprocessing steps
        selected_columns = ['Sunshine', 'Humidity9am', 'Humidity3pm', 'Cloud9am', 'Cloud3pm', 'RainToday']
        df_selected = df[selected_columns]
        if df_selected['RainToday'].value_counts().min() != df_selected['RainToday'].value_counts().max():
            X_resampled, y_resampled = SMOTE(random_state=42).fit_resample(
                df_selected.drop(columns=['RainToday']), df_selected['RainToday'])
            df_balanced = pd.DataFrame(X_resampled, columns=df_selected.columns[:-1])
            df_balanced['RainToday'] = y_resampled
        else:
            df_balanced = df_selected

        preprocessor = ColumnTransformer(transformers=[
            ('cat', OneHotEncoder(), ['Cloud9am', 'Cloud3pm']),
            ('num', StandardScaler(), ['Sunshine', 'Humidity9am', 'Humidity3pm'])
        ], remainder='passthrough')
        X_train, X_test, y_train, y_test = train_test_split(df_balanced.drop(columns=['RainToday']), df_balanced['RainToday'], test_size=0.3, random_state=42)
        pipeline = ImbPipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', DecisionTreeClassifier(criterion="entropy", max_depth=4, random_state=42))
        ])
        pipeline.fit(X_train, y_train)
        return pipeline, X_test, y_test

    @task
    def evaluate_model(model, X_test, y_test):
        # Evaluate the model using f1_score
        y_pred = model.predict(X_test)
        return f1_score(y_test, y_pred)

    @task
    def save_and_log_model(model, f1):
        # Log model and metrics to MLflow
        local_model_path = "/opt/airflow/dags/files/model.pkl"
        with open(local_model_path, 'wb') as file:
            pickle.dump(model, file)
        with mlflow.start_run():
            mlflow.sklearn.log_model(model, "model")
            mlflow.log_metric("test_f1", f1)

    # Define the flow of tasks
    df_rain = load_data()
    model, X_test, y_test = preprocess_data(df_rain)
    f1 = evaluate_model(model, X_test, y_test)
    save_and_log_model(model, f1)

# Instantiate the DAG
data_science_pipeline_dag = data_science_pipeline()
