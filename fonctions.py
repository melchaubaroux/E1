from bdd import db_connexion

# Connect to MongoDB
db, collection = db_connexion()

#Fetch documents from the collection
def all_documents():
    documents = list(collection.find())
    return documents

# user 1 le nombre de livre total
def nb_books():
    total=collection.count_documents({})
    return total

# user 2 : nombre de livre dans chaque categorie tri descendant
def nb_liv_categ():
    aggregate_type_count = collection.aggregate([
            {"$group": {"_id": "$category", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ])
    dict_categ={result['_id']:result['count'] for result in aggregate_type_count}
    return dict_categ

# user 3 : chercher un livre particulier
def chercher_livre(titre):
    doc=collection.find_one({'title': titre})
    return doc

# user 4 : top livre moins cher 
def top10_moins_cher():
    search=list(collection.find({}).sort("Price").limit(10))
    print(*list([x['category'],x['title'] ,x["Price"]] for x in search),sep='\n')

    pass

# user 5 : top livre plus cher 
def top10_plus_cher():
    search=list(collection.find({}).sort("Price",-1).limit(10))
    print(*list([x['category'],x['title'] ,x["Price"]] for x in search),sep='\n')
    pass

# user 6 : moyenne des etoiles totales et moyennes des étoiles par ctaégories

etoile_value={'One':1,'Two':2,'Three':3,'Four':4,'Five':5}
star_repartition = collection.aggregate([
    {"$group": {"_id": "$star", "count": {"$sum": 1}}}
])

star_total=sum([etoile_value[element["_id"]]*element["count"] for element in star_repartition ])
print(star_total)
print(star_total/collection.estimated_document_count())

# user 7 : top note +
#idem user 6

# user 8 : faire des recherches sur une categorie
    
# user 9 : liste des titres à moins ou plus de XX€




#ci-dessous essais de requetes simples
#print(db.list_collection_names())

#print(collection.find_one({"title":'Soumission'}))

#print(collection.find_one({"star":'One'}))

#response=collection.find({'star': 'One'})
#for doc in response:
#    print(doc)
#print([doc for doc in response])

# liste et compte des livres avec 5 étoiles
# response=collection.find({'star': 'Five'})
# print([doc['title'] for doc in response])   
# print("Nombre de livre avec 5 étoiles: " + str(collection.count_documents({'star': 'Five'})))

# liste et compte des livres avec 5 étoiles
# response=collection.find({'$and': [{'star': 'Five'}, {'category': 'Mystery'}]})
# print([doc['title'] for doc in response])
# print("Nombre de livre avec 5 étoiles et categorie Mystery: " + str(collection.count_documents(
#     {'$and': [{'star': 'Five'}, {'category': 'Mystery'}]}
# )))

# print("Nombre de livre avec 5 étoiles OU categorie Mystery: " + str(collection.count_documents(
#     {'$or': [{'star': 'Five'}, {'category': 'Mystery'}]}
# )))

