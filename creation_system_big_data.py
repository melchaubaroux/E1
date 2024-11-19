import os
import tempfile

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
import pandas as pd

from pyspark.sql import SparkSession

import uvicorn

# Créer l'application FastAPI
app = FastAPI()

# Créer une session Spark pour interagir avec les données Big Data
spark = SparkSession.builder.appName("MultiTableBigDataSystem").getOrCreate()

# Chemin du dossier contenant les fichiers CSV
folder_CSV_path = "file_csv"

# Chemin du dossier contenant les fichiers Parquet
folder_Parquet_path = "file_parquet"

# Liste pour stocker les noms des tables chargées
table_list = []

# Fonction pour vérifier si un fichier ou un répertoire existe à un chemin donné
def check_file_exists(file_path):
    """
    Vérifie si le fichier ou le répertoire existe à l'adresse donnée.
    """
    if os.path.exists(file_path):
        print(f"L'adresse '{file_path}' existe.")
        return True
    else:
        print(f"L'adresse '{file_path}' n'existe pas.")
        return False

# Convertir chaque fichier CSV en Parquet si nécessaire
for filename in os.listdir(folder_CSV_path):
    # Vérifier si le fichier est un CSV
    if filename.endswith(".csv"):
        # Définir le chemin du fichier Parquet à partir du fichier CSV
        parquet_path = os.path.join(folder_Parquet_path, filename.replace(".csv", ".parquet"))

        # Si le fichier Parquet n'existe pas déjà, on procède à la conversion
        if not check_file_exists(parquet_path):
            # Lire le fichier CSV avec Spark
            df = spark.read.csv(os.path.join(folder_CSV_path, filename), header=True, inferSchema=True)

            # Sauvegarder le DataFrame sous format Parquet pour une utilisation plus rapide et efficace
            df.write.parquet(parquet_path)



# Fonction pour charger un fichier CSV dans Spark et créer une vue temporaire avec ce fichier
def load_csv_and_create_view(file_path: str, table_name: str):
    """
    Charge un fichier CSV dans Spark et crée une vue temporaire accessible via SQL.
    """
    # Lire le fichier CSV avec Spark
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Créer une vue temporaire dans Spark avec le nom de la table
    df.createOrReplaceTempView(table_name)
    return df

# Charger les fichiers Parquet en mémoire et les convertir en vues temporaires
for filename in os.listdir(folder_Parquet_path):
    # Vérifier si le fichier est un fichier Parquet
    if filename.endswith(".parquet"):
        # Nom de la table est basé sur le nom du fichier sans l'extension
        table_name = filename.split(".parquet")[0]

        # Obtenir le chemin complet du fichier Parquet
        file_path = os.path.join(folder_Parquet_path, filename)

        # Lire le fichier Parquet dans Spark
        df = spark.read.parquet(file_path)

        # Créer une vue temporaire à partir du DataFrame chargé
        df.createOrReplaceTempView(table_name)

        # Ajouter le nom de la table à la liste des tables disponibles
        table_list.append(table_name)

# Modèle Pydantic pour recevoir les requêtes JSON
class QueryRequest(BaseModel):
    """
    Modèle de données pour recevoir une requête SQL via une API POST.
    """
    table_name: str=""  # Le nom de la table (fichier CSV ou Parquet)
    col_name : str ="" # le nom des colonnes 
    query: str=""      # La requête SQL à exécuter sur la table spécifiée et colonne specifié

# Endpoint pour exécuter une requête SQL sur les données chargées dans Spark

@app.get("/list_of_table")
def list_of_table() : 
    return table_list


@app.post("/query")
async def query_csv(request: QueryRequest):
    """
    Ce point d'API permet d'exécuter des requêtes SQL sur les tables chargées en mémoire.
    Il prend un nom de table et une requête SQL en entrée, et renvoie un fichier CSV avec le résultat.
    """

    if request.col_name=="" : request.col_name="*"

    # Si "all" est spécifié, interroger toutes les tables
    if request.table_name == "":
        final_result_df = []
        for table in table_list:

            #Requete de base 
            query = f"SELECT {request.col_name} FROM '{table}' "

            # Completion de la requete 
            if request.query !="": query+=request.query 

            # Application de la requete
            try:
                result_df = spark.sql(query)
                final_result_df.append(result_df)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Erreur avec la table {table}: {str(e)}")

        # Fusionner les résultats
        # if combined_results:
        #     final_result_df = combined_results[0]
        #     for df in combined_results[1:]:
        #         final_result_df = final_result_df.union(df)
        # else:
        #     raise HTTPException(status_code=404, detail="Aucune donnée trouvée pour les tables sélectionnées.")
    
    elif request.table_name in table_list:

        if request.col_name=="" : request.col_name="*"

        #Requete de base 
        query = f"SELECT {request.col_name} FROM {request.table_name} "

        # Completion de la requete 
        if request.query !="": query+=request.query 

        # Application de la requete
        try:
            final_result_df = spark.sql(query)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Erreur dans la requête : {str(e)}")
    else:
        raise HTTPException(status_code=404, detail=f"Table '{request.table_name}' inexistante.")
    

    

    if not final_result_df : raise HTTPException(status_code=404, detail="Aucune donnée trouvée pour les tables sélectionnées.")

    result_pd = final_result_df.toPandas()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
        result_pd.to_csv(tmp_file.name, index=False)
        return FileResponse(tmp_file.name, media_type='text/csv', filename="result.csv")
    


if __name__=="__main__":

    uvicorn.run(app,host='0.0.0.0', port=2000)