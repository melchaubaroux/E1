from pymongo import MongoClient
import time

def db_connexion():
    start = time.time()
    # Connexion à MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["books_scraped"]
    collection= db["books"]
    end = time.time()
    print('Connecté à la bdd en:' + str(end - start))
    return db, collection