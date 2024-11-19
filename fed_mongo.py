import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import re
from fonctions_scrapping import extract_info_of_book,extract_book_links,extract_pages_links



if __name__=="__main__": 

    # Obtenez le nombre de cœurs (cores) disponibles sur la machine
    import os
    num_cores = os.cpu_count()
    print(f"Nombre de cœurs disponibles : {num_cores}")

    #recuperation des liens de toutes les pages de la categorie all books qui contient tous les livres du site 
    urls=extract_pages_links()

    # recuperation des url de livres de toutes les pages 

    from concurrent.futures import ThreadPoolExecutor

    all_links=[]
    if urls: 
        # Nombre de threads à utiliser (ajustez selon votre besoin)
        num_threads = num_cores-1

        # Utilisation de ThreadPoolExecutor pour paralléliser les requêtes
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Lancez les requêtes en parallèle
            all_links= executor.map(extract_book_links, urls)
            
    
    # aplatissement de la liste 
    import itertools 
    all_book_links = list(itertools.chain.from_iterable(all_links))


    # correction des liens 
    all_links=[link.replace("../..", "https://books.toscrape.com/catalogue") for link in all_book_links]


    from pymongo import MongoClient
    import time

    try:
        start = time.time()
        # Connexion à MongoDB
        client = MongoClient("mongodb://localhost:27017/")
        db = client["books"]
        collection = db["books"]
        end = time.time()
        print('Connecté à la bdd en:' + str(end - start))
    except:
        print("erreur de la connection mongo")
        # raise HTTPException(status_code=500)



    def send_info_book (url):
        to_send=extract_info_of_book(url)

        objet=collection.find({'title':to_send['title']})
        
        if len(list(objet))!=0 : 
            collection.update_one({'title':to_send['title']},{'$set':to_send})
        else : 
            collection.insert_one(to_send)



    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(send_info_book, all_links)