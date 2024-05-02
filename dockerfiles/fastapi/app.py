import json
import pickle
import boto3
import mlflow
import pandas as pd
import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Literal

# Initialize MLflow tracking URI
mlflow.set_tracking_uri('http://mlflow:5000')

app = FastAPI()

# Function to load the model and data dictionary
def load_model(model_name: str, alias: str):
    client_mlflow = mlflow.MlflowClient()
    current_dir = os.path.dirname(os.path.realpath(__file__))  # Absolute path to current directory
    
    try:
        # Attempt to fetch the model by alias
        model_data_mlflow = client_mlflow.get_model_version_by_name_and_stage(model_name, alias)
        model_ml = mlflow.sklearn.load_model(model_data_mlflow.source)
        version_model_ml = int(model_data_mlflow.version)
    except Exception as e:
        # If MLflow call fails, load the backup model
        model_path = os.path.join(current_dir, 'files', 'model.pkl')
        with open(model_path, 'rb') as file_ml:
            model_ml = pickle.load(file_ml)
        version_model_ml = 0

    try:
        # Attempt to fetch data dictionary from S3
        s3 = boto3.client('s3')
        result_s3 = s3.get_object(Bucket='data', Key='data_info/datas.json')
        text_s3 = result_s3["Body"].read().decode()
        data_dictionary = json.loads(text_s3)
    except Exception as e:
        # If S3 call fails, load the backup data dictionary
        data_path = os.path.join(current_dir, 'files', 'data.json')
        with open(data_path, 'r') as file_s3:
            data_dictionary = json.load(file_s3)

    return model_ml, version_model_ml, data_dictionary

# Define the Pydantic models for input
class ModelInput(BaseModel):
    Sunshine: float = Field(ge=0, le=24)
    Humidity9am: float = Field(ge=0, le=100)
    Humidity3pm: float = Field(ge=0, le=100)
    Cloud9am: float = Field(ge=0, le=10)
    Cloud3pm: float = Field(ge=0, le=10)

# Define the Pydantic model for output
class ModelOutput(BaseModel):
    int_output: bool = Field(description="Indicates whether it is expected to rain tomorrow.")
    str_output: Literal["Tomorrow Rains", "No Rain"] = Field(description="Descriptive output of the model regarding tomorrow's weather.")

# Endpoint to retrieve predictions
@app.post("/predict/", response_model=ModelOutput)
async def predict(features: ModelInput):
    # Load the model and data dictionary
    model, version_model, data_dict = load_model("Lluvia_model_prod2", "champion")

    # Extract feature names from the data dictionary or define manually if necessary
    feature_names = data_dict.get('columns', [])

    # Create a DataFrame from the input features ensuring order and naming
    features_dict = features.dict()
    ordered_features = {name: [features_dict[name]] for name in feature_names}

    features_df = pd.DataFrame(ordered_features)

    # Ensure that the DataFrame columns match the training features
    if not all(features_df.columns == feature_names):
        missing_features = set(feature_names) - set(features_df.columns)
        additional_features = set(features_df.columns) - set(feature_names)
        error_message = f"Feature mismatch error: Missing features {missing_features}, Additional features {additional_features}"
        raise HTTPException(status_code=422, detail=error_message)

    # Predict using the loaded model
    prediction = model.predict(features_df)

    # Process the prediction
    pred_result = bool(prediction[0])

    # Return the prediction as ModelOutput
    return ModelOutput(int_output=pred_result, str_output="Tomorrow Rains" if pred_result else "No Rain")

# This is for running locally, remove if deploying
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8800, reload=True)
