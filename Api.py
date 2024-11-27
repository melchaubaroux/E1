from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
import pandas as pd
import io
import uvicorn

# Créer l'application FastAPI
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Charger le DataFrame nettoyé
df_cleaned = pd.read_csv('cleaned_data/dataset.csv')  # Ou directement à partir du DataFrame `df_cleaned`

@app.get("/data")
def get_data():
    """
    Endpoint pour récupérer les données sous forme de JSON.
    """
    # Convertir le DataFrame en dictionnaire (list of dicts) pour l'API
    data = df_cleaned.to_dict(orient='records')
    return JSONResponse(content=data)

@app.get("/download")
def download_file():
    """
    Endpoint pour télécharger les données sous forme de CSV.
    """
    # Convertir le DataFrame en CSV
    csv_data = df_cleaned.to_csv(index=False)
    return StreamingResponse(io.StringIO(csv_data), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=data.csv"})





if __name__ == '__main__':
    uvicorn.run(app,host="0.0.0.0" , port=8001)



