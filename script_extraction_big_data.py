import os
import shutil

# Importer la bibliothèque nécessaire pour Spark
from pyspark.sql import SparkSession

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("RequeteSparkSansAPI") \
    .master("local[*]") \
    .getOrCreate()

# Chemin vers le dossier contenant les fichiers Parquet
# Utilisez "./parquet_data" si le script est exécuté depuis le même dossier
chemin_parquet = "file_parquet"

table_list=[]

# Charger les fichiers Parquet en mémoire et les convertir en vues temporaires
for filename in os.listdir(chemin_parquet):
    # Vérifier si le fichier est un fichier Parquet
    if filename.endswith(".parquet"):
        # Nom de la table est basé sur le nom du fichier sans l'extension
        table_name = filename.split(".parquet")[0]

        # Obtenir le chemin complet du fichier Parquet
        file_path = os.path.join(chemin_parquet, filename)

        # Lire le fichier Parquet dans Spark
        df = spark.read.parquet(file_path)

        # Créer une vue temporaire à partir du DataFrame chargé
        df.createOrReplaceTempView(table_name)

        # Ajouter le nom de la table à la liste des tables disponibles
        table_list.append(table_name)

requested_table="netflix_movies"

requete_sql = f"""
    SELECT *
    FROM {requested_table}
    
"""

# Exécuter la requête et récupérer les résultats dans un DataFrame Spark
resultat = spark.sql(requete_sql)

assert resultat != None

# Afficher les résultats dans la console
# resultat.show()

# Optionnel : Enregistrer les résultats dans un fichier CSV
chemin_sortie = "extracted_data/netflix.csv"  # Chemin de sortie du fichier CSV
resultat.coalesce(1).write.mode("overwrite").option("header", "true").csv(chemin_sortie)

# Stopper la session Spark pour libérer les ressources
spark.stop()


# post traitment de l'extraction pour mise en forme de csv pur 

# Spécifier le chemin du dossier contenant les fichiers CSV générés
chemin_dossier_csv =chemin_sortie  # Remplacez par le chemin réel du dossier contenant les fichiers CSV
chemin_donnees_extraite = "extracted_data"  # Dossier où le fichier sera stocké
nom_table = requested_table  # Le nom de la table d'origine à ajouter dans les données

# Vérifier que le dossier contenant les fichiers CSV existe
if os.path.exists(chemin_dossier_csv):
    # Chercher le vrai fichier CSV dans le dossier (fichier part-00000-...)
    fichiers_csv = [f for f in os.listdir(chemin_dossier_csv) if f.startswith("part-")]
    
    if fichiers_csv:
        fichier_csv = fichiers_csv[0]  # Choisir le premier fichier "part-00000"
        chemin_fichier_csv = os.path.join(chemin_dossier_csv, fichier_csv)
        
        
        # Spécifier le chemin de destination pour l'exportation
        chemin_export = os.path.join(chemin_donnees_extraite, f"{nom_table}.csv")

        os.rename(chemin_fichier_csv,chemin_export)
        shutil.rmtree(chemin_dossier_csv)
        
        
    else:
        print(f"Aucun fichier CSV trouvé dans le dossier {chemin_dossier_csv}")
else:
    print(f"Le dossier {chemin_dossier_csv} n'existe pas.")