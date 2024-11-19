import requests
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor
import itertools 





# extraire les données d'une page d'un livre 
def extract_info_of_book(book_url):

        response = requests.get(book_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        title = soup.find('h1').text.strip()
        description = soup.find('meta', attrs={"name": "description"})['content'].replace("\\n ","").replace("  "," ")
        features=[f.string for f in soup.select(' table tr th')]
        values=[v.string for v in soup.select('table tr td')]
        etoile_value={'One':1,'Two':2,'Three':3,'Four':4,'Five':5}
        star =etoile_value[soup.find_all(attrs={'class':re.compile("^star")})[0]['class'][1]]

        useless_features=0

        for i in range (len(features)):
            if 'Price' in features[i] or 'Tax' in features[i] : 
                currency=values[i][1]
                break 

        for i in range (len(features)):
            if 'Price' in features[i] or 'Tax' in features[i] : 
                values[i]=float(values[i][2:])
            if 'Availability' in features[i] :
                stock=int(''.join([x for x in values[i] if x.isdigit()]))
               
                values[i]=stock
            if features[i]=="Price (excl. tax)": 
                useless_features=i
            if features[i]=="Price (incl. tax)": 
                features[i]="Price"
            if 'reviews' in features[i] : 
                values[i]=int(values[i])

        
        features.pop(useless_features)
        values.pop(useless_features)
            
        category= soup.select('a')[3].string    


        info_book={'category':category,'title':title,'star':star}
        info_book['description']=description
        info_book.update({k:v for k,v in zip(features,values)})
        info_book['description']=description
        info_book['currency']=currency

        
        return info_book

#  fonction de recuparation des url de livre  depuis un page de  la categorie principal 
def extract_book_links (url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        # book_links=soup.select('div.image_container a ')
        book_links= [ a['href'] for a in soup.select(' article div a')]
        book_links

        return  book_links

            
    except Exception as e:
        print(f"Erreur lors de l'extraction des liens pour {url}: {str(e)}")

        return None


#fonction qui recupere les urls des pages de la categorie principal 
def extract_pages_links ():
        try :
                response = requests.get('https://books.toscrape.com/catalogue/category/books_1')
                soup = BeautifulSoup(response.text, 'html.parser')
                number_of_pages=int(soup.select('li.current')[0].string.split()[-1])
                urls=[f"https://books.toscrape.com/catalogue/category/books_1/page-{x}.html" for x in range(1,number_of_pages+1)]
                return urls

        except Exception as e:
                print(f"Erreur lors de la connection a https://books.toscrape.com: {str(e)}")
                return None
                



# V2 Fonction principale pour scraper le site
def scrape_books():
    #recuperation des liens de toutes les pages de la categorie all books qui contient tous les livres du site 
    urls=extract_pages_links()

    if urls :
        # recuperation des url de livres de toutes les pages 
        all_links=[]
    
        # Nombre de threads à utiliser (ajustez selon votre besoin)
        num_threads = num_cores-1

        # Utilisation de ThreadPoolExecutor pour paralléliser les requêtes
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Lancez les requêtes en parallèle
            all_links= executor.map(extract_book_links, urls)

        
        # aplatissement de la liste 
        
        all_book_links = list(itertools.chain.from_iterable(all_links))

        # correction des liens 
        all_links=[link.replace("../..", "https://books.toscrape.com/catalogue") for link in all_book_links]


        #recuperation des infos sur les livres 
        info=[]
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            info+= executor.map(extract_info_of_book, all_links)
                    
        return info 




##########################################################################################"""
# fonction devenue inutile  


# Fonction pour extraire les  liens de toutes les pages de la categorie principal
def exctract_page_links():
    url = 'https://books.toscrape.com/catalogue/category/books_1'
   
    page_links = []  # dict pour stocker les titres de tous les livres de la catégorie
    current_page = 1

    while True:
        
        current_page_url = f"{url}/page-{current_page}.html"
        test_of_existence = requests.get(current_page_url,stream=True)
        # print(current_page)

        if not test_of_existence: break  
        else : page_links+=[current_page_url]

        current_page += 1
    
    return page_links



# Fonction pour extraire les infos des livres d'une page donnée
def extract_info_from_page(page_url):
        response = requests.get(page_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        books=[]

        books_url= [ a['href'] for a in soup.select(' article div a')]
        
        for book_url in books_url :
                reverse_url=book_url[::-1]
                suffixe=book_url[::-1][:reverse_url.index('/..')]
                b=suffixe[::-1]

                


                info=extract_info_of_book('https://books.toscrape.com/catalogue/'+b)
                books[info['title']]=info
        
        return  books



# Fonction pour extraire les informations d'une catégorie donnée avec gestion de la pagination
def extract_category_info(category_url):
    
    response = requests.get(category_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    category_title = soup.find('h1').text.strip()
    print(category_title)

    books_titles = {}  # dict pour stocker les titres de tous les livres de la catégorie

    # Première page de la catégorie
    current_page_url = f"{category_url}/index.html"
    books_on_page = extract_info_from_page(current_page_url)
    books_titles.update(books_on_page)

    
    current_page = 2

    # Boucle pour parcourir toutes les pages de la catégorie
    while True:
        #print(current_page)
        current_page_url = f"{category_url}/page-{current_page}.html"
        books_on_page = extract_info_from_page(current_page_url)
        

        if not books_on_page:
            break  # Si la page est vide, sortir de la boucle

        
        books_titles.update(books_on_page)
        current_page += 1
        
    return category_title,books_titles



# V1 Fonction principale pour scraper le site
def scrape_books():
    url = 'https://books.toscrape.com/'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    
    # Extraire les liens des catégories
    category_links = [url + a['href'].replace("index.html","") for a in soup.select('div.side_categories ul.nav-list li ul li a')]
    print(len(category_links))
    
    # Initialiser le dictionnaire
    books_dict = {}


    # Parcourir les catégories et extraire les informations
    for category_link in tqdm(category_links):
        #print(category_link)
        category_title, book_titles = extract_category_info(category_link)#,book_description
        #print(category_title, len(book_titles ))
        books_dict[category_title] = book_titles
         
    return books_dict








