"""
dans le des csv on a l'information des differentes categorie de données , idem pour les json , pour autant si des sources ont des nomenclature differentes 
alors que  c'est sur le même sujet , une aggregation brut considererais que ce sont des sujet different , cela implique que le systeme de doit pas etre sensible a la casse

il est peut etre necessaire d'avoir des fonctions qui analyse les documents en amont.

utilisation de panda peut etre interressant pour la facilité de manipulation 


"""

import os 

import pandas as pd 

#fonction generique 

"""
def check_type (data, attente) :
    for x in info :
        assert type(x) == type attendu   


def type_of source (sources:list): 
    detecte le type de source pour chaque membre de la liste  
    appel via dictionnnaire avec un try la fonction correspondante  ou renvoi 'info de nom prise en charge 


  
"""

#recuperation des donnée du dossier exctraction 

#  liste les adresse des  fichiers 
path_file_to_load=[filename for filename in os.listdir("extracted_data") ]

# liste les nomdes fichiers 
name_file_to_load = [filename.split(".csv")[0] for filename in path_file_to_load]

loaded_file = [pd.read_csv(pathfile) for pathfile in path_file_to_load]

# ouvre les fichiers est les stocke dans un dictionnaire 
datasets = {k: v for k, v in zip(name_file_to_load, loaded_file)}

print (datasets[name_file_to_load[0]])






"""
"""

# suppression des données corrompu


# suppression des sections inutiles 



# normalisation

"""
dans le cas des tableaux des types d'informations sont attendu , par exemple la colonne année doit etre un entier,
il faut donc verifier que la relation informmations type de stockage est correcte 

def check type is good (info , type attendu ) : 
    for x in info :
        assert type(x) == type attendu 
    
    
"""

# agreggation 


""" 
 il faut mettre les informations dans une même table , ce qui implique d'avoir determiner les colonnes  et les types voulu ,
   donc avoir regrouper plusieurs colonne de source differentes sous un même nom 
   => gestion de la casse pour les correspondance 
   => concordance des types 
   => modification special si il y en a 
"""

# exportation 

"""
integration dans la base de données 
"""


if __name__=="main" : 
    """
        appel des processus decrit plus haut 
    """