<<<<<<< HEAD
from fastapi import FastAPI


app = FastAPI()


@app.get("/")
def read_root():
    return {"message": "Welcome to the Model Service"}
=======
import os
import pickle
import pandas as pd
import boto3
import mlflow

from fastapi import FastAPI
from pydantic import BaseModel, Field

os.environ['AWS_ACCESS_KEY_ID'] = 'minio'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'minio123'
os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'http://localhost:9000'
os.environ['AWS_ENDPOINT_URL_S3'] = 'http://localhost:9000'

class ModelInput(BaseModel):
    Sunshine: float = Field(ge=0, le=24)
    Humidity9am: float = Field(ge=0, le=100)
    Humidity3pm: float = Field(ge=0, le=100)
    Cloud9am: float = Field(ge=0, le=10)
    Cloud3pm: float = Field(ge=0, le=10)

def load_model():
    model_name = "Lluvia_model_prod2"
    alias = "champion1"
    try:
        mlflow.set_tracking_uri('http://mlflow:5000')
        client_mlflow = mlflow.MlflowClient()
        model_data_mlflow = client_mlflow.get_model_version_by_alias(model_name, alias)
        model_ml = mlflow.sklearn.load_model(model_data_mlflow.source)
        version_model_ml = int(model_data_mlflow.version)
    except:
        file_ml = open('/app/files/model.pkl', 'rb')
        model_ml = pickle.load(file_ml)
        file_ml.close()
        version_model_ml = 0
    return model_ml

app = FastAPI()
model = load_model()

@app.post("/predict/", response_model=bool)
async def predict(input: ModelInput):
    features_df = pd.DataFrame([input.dict()])
    prediction = model.predict(features_df)
    return bool(prediction[0])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8800, reload=True)
>>>>>>> example_implementation
