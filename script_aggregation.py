"""
dans le des csv on a l'information des differentes categorie de données , idem pour les json , pour autant si des sources ont des nomenclature differentes 
alors que  c'est sur le même sujet , une aggregation brut considererais que ce sont des sujet different , cela implique que le systeme de doit pas etre sensible a la casse

il est peut etre necessaire d'avoir des fonctions qui analyse les documents en amont.

utilisation de panda peut etre interressant pour la facilité de manipulation 

def check_type (data, attente) :
    for x in info :
        assert type(x) == type attendu   


def type_of source (sources:list): 
    detecte le type de source pour chaque membre de la liste  
    appel via dictionnnaire avec un try la fonction correspondante  ou renvoi 'info de nom prise en charge 

"""

import os 
import pandas as pd 
import numpy as np

#recuperation des donnée du dossier exctraction 


#  liste les adresse des  fichiers 
path_file_to_load= ["extracted_data/"+filename for filename in os.listdir("extracted_data") ]

# liste les nomdes fichiers 
name_file_to_load = [filename.split(".csv")[0] for filename in path_file_to_load] 

loaded_file = [pd.DataFrame(pd.read_csv(pathfile)) for pathfile in path_file_to_load]

# ouvre les fichiers est les stocke dans un dictionnaire 

datasets = {k: v for k, v in zip(name_file_to_load, loaded_file)}

# Harmoniser les noms des colonnes (tout en minuscules)
for x in datasets : 
    datasets[x].columns=datasets[x].columns.str.lower()
    

# Liste de DataFrames
list_datasets = [datasets[x] for x in datasets]  # Liste contenant plusieurs DataFrames

# Fusion successive 
merged_df = list_datasets[0]  # Commencer avec le premier DataFrame
for df in list_datasets[1:]:
    merged_df = pd.merge(merged_df, df, how='outer')  # Utilise "inner"(keep common ligne) ou "outer" (keep all ligne) selon les besoins

# suppression des sections inutiles 

df_selected = merged_df[['title', 'genre', 'description','rating', 'votes']]


# suppression des données corrompu

# 1. Vérifier les types et convertir si nécessaire
df_selected['rating'] = pd.to_numeric(df_selected['rating'], errors='coerce')  # Convertit en numérique, remplace les erreurs par NaN
df_selected['votes'] = pd.to_numeric(df_selected['votes'], errors='coerce')

df_selected['title'] = df_selected['title'].apply(lambda x: np.nan if not isinstance(x, str) else x)
df_selected['genre'] = df_selected['genre'].apply(lambda x: np.nan if not isinstance(x, str) else x)
df_selected['description'] = df_selected['description'].apply(lambda x: np.nan if not isinstance(x, str) else x)

# .2 Supprimer les lignes avec des valeurs manquantes dans les colonnes essentielles
df_cleaned = df_selected.dropna(subset=['title', 'genre', 'description','rating', 'votes'])



# normalisation

#  Supprimer les lignes avec des valeurs aberrantes
df_cleaned = df_cleaned[(df_cleaned['rating'] >= 0) & (df_cleaned['rating'] <= 10)]  # Garder les notes entre 0 et 10
df_cleaned = df_cleaned[df_cleaned['votes'] > 0]  # Votes positifs uniquement




# exportation 

df_cleaned.to_csv('cleaned_data/dataset.csv', index=False)