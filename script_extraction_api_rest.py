import requests
import csv
import os
from pydantic import BaseModel

# Définition de la structure de la requête
class QueryRequest(BaseModel):
    table_name: str  # Chemin du fichier CSV
    query: str        # Requête SQL à exécuter

# Fonction d'extraction des données
def extract_data(query_request: QueryRequest):
    # URL de l'API
    url = 'http://localhost:2000/query'

    # Envoi de la requête POST
    response = requests.post(url, json=query_request.dict())

    # Vérification de la réponse
    if response.status_code == 200:
        # Si la requête est réussie, récupérer le contenu CSV
        csv_content = response.content

        # Création du dossier "extracted_data" si nécessaire
        os.makedirs("extracted_data", exist_ok=True)

        # Définir le chemin du fichier de sortie
        output_file = os.path.join("extracted_data", f"{query_request.table_name}.csv")

        # Sauvegarder les données CSV dans le fichier
        with open(output_file, 'wb') as f:
            f.write(csv_content)

        print(f"Les résultats ont été sauvegardés dans {output_file}")
    else:
        print(f"Erreur lors de la requête : {response.status_code} - {response.text}")

# Exemple d'utilisation
if __name__ == "__main__":
    # Exemple de paramètre de requête
    query_request = QueryRequest(
        table_name="imdb_movies",  # Nom du fichier CSV ou chemin
        query=""  # Requête SQL
    )

    # Exécution de la fonction d'extraction
    extract_data(query_request)
