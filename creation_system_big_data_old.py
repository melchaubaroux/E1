import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
import pandas as pd

from pyspark.sql import SparkSession

# Créer l'application FastAPI
app = FastAPI()

# creation de la session Spark
spark = SparkSession.builder.appName("MultiTableBigDataSystem").getOrCreate()


# Chemin du dossier contenant les CSV
folder_CSV_path = "path/to/csv/folder"

# Chemin du dossier contenant les Parquets
folder_Parquet_path = "path/to/parquet/folder"


# Verifie l'existance d'un objet a l'adresse de chemin fournit 
def check_file_exists(file_path):
    if os.path.exists(file_path):
        print(f"L'adresse '{file_path}' existe.")
        return True
    else:
        print(f"L'adresse '{file_path}' n'existe pas.")
        return False


# Convertir chaque fichier CSV en Parquet
for filename in os.listdir(folder_CSV_path):
    if filename.endswith(".csv"):

        # Définir un chemin pour le fichier Parquet
        parquet_path = os.path.join("path/to/parquet/folder", filename.replace(".csv", ".parquet"))

        if not check_file_exists(parquet_path):

            # Lire le fichier CSV
            df = spark.read.csv(parquet_path, header=True, inferSchema=True)
            
            # Sauvegarder en format Parquet
            df.write.parquet(parquet_path)



# on creer une liste qui reference les tables 
table_list=[]


# Charger un fichier CSV dans Spark et créer une vue temporaire
def load_csv_and_create_view(file_path: str, table_name: str):
    # Lire le fichier CSV
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    # Utiliser le nom du fichier comme nom de la vue temporaire
    df.createOrReplaceTempView(table_name)
    return df


## Charger chaque fichier comme une table distincte
# for filename in os.listdir(folder_CSV_path):  
#     if filename.endswith(".csv"):
#         # Nom de la table sans l'extension
#         table_name = filename.split(".csv")[0] 
#         # chemin complet du fichier
#         file_path = os.path.join(folder_CSV_path, filename)

#         # Charger un fichier CSV dans Spark, créer une vue temporaire
#         df = load_csv_and_create_view(file_path, table_name)

#         # on rajoute la table a la liste 
#         table_list+=[table_name]

# Charger les fichiers Parquet et créer des vues temporaires


for filename in os.listdir(folder_Parquet_path):
    if filename.endswith(".parquet"):

        # Nom de la table sans l'extension
        table_name = filename.split(".parquet")[0]

        # chemin complet du fichier
        file_path = os.path.join(folder_Parquet_path, filename)

        # Lire le fichier Parquet
        df = spark.read.parquet(file_path)

        # Charger un fichier Parquet dans Spark, créer une vue temporaire
        df.createOrReplaceTempView(table_name)

        # on rajoute la table a la liste 
        table_list+=[table_name]


    



# Modèle Pydantic pour recevoir les requêtes JSON
class QueryRequest(BaseModel):
    table_name: str  # Chemin du fichier CSV
    query: str      # Requête SQL à exécuter


# Endpoint pour exécuter la requête SQL
@app.post("/query")
async def query_csv(request: QueryRequest):

    if request.table_name not in table_list:
        raise HTTPException(status_code=404, detail=f"Table '{request.table_name}' does not exist.")
    
    # Construire la requête SQL pour interroger la table spécifiée
    query = f"SELECT * FROM {request.table_name} WHERE {request.query}"
    
   
    # Exécuter la requête SQL sur la vue temporaire
    
    try:
        result_df = spark.sql(query)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur dans l'exécution de la requête : {str(e)}")
    
    # Convertir le résultat Spark DataFrame en un DataFrame Pandas pour l'exportation en CSV
    result_pd = result_df.toPandas()

    # Sauvegarder le résultat dans un fichier CSV
    result_csv_path = "/tmp/result.csv"
    result_pd.to_csv(result_csv_path, index=False)
    
    # Retourner le fichier CSV
    return FileResponse(result_csv_path, media_type='text/csv', filename="result.csv")
