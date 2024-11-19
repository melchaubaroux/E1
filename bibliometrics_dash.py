import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from pymongo import MongoClient

from fonctions import all_documents, nb_books, nb_liv_categ, chercher_livre

# Initialiser l'application Dash
app = dash.Dash(__name__)

# Initialiser l'application Dash
app = dash.Dash(__name__)

# Mise en page du tableau de bord avec des onglets
app.layout = html.Div([
    html.H1("Tableau de Bord Livres"),

    dcc.Tabs(id='tabs', value='total_books', children=[
        dcc.Tab(label='Nombre Total de Livres', value='total_books'),
        dcc.Tab(label='Nombre de Livres par Catégorie', value='books_by_category'),
        dcc.Tab(label='Rechercher un Livre', value='search_book'),
        dcc.Tab(label='Top Livres Moins Chers', value='top_cheapest_books'),
        dcc.Tab(label='Top Livres Plus Chers', value='top_expensive_books'),
        dcc.Tab(label='Top Livres avec Note -', value='top_lowest_rated_books'),
        dcc.Tab(label='Top Livres avec Note +', value='top_highest_rated_books'),
        dcc.Tab(label='Recherche dans une Catégorie', value='category_search'),
    ]),

    # Contenu des onglets
    html.Div(id='tab-content')
])

# Callback pour mettre à jour le contenu des onglets
@app.callback(
    Output('tab-content', 'children'),
    [Input('tabs', 'value')]
)
def update_tab_content(selected_tab):
    if selected_tab == 'total_books':
        # Requête 1: Le nombre total de livres
        total_books = nb_books()
        return html.Div(f"Nombre total de livres : {total_books}")

    elif selected_tab == 'books_by_category':
        
        # Requête 2: Le nombre de livres dans chaque catégorie
        dict = nb_liv_categ()
        categ = list(dict.keys())
        val = list(dict.values())
        bar_chart = px.bar(x=categ, y=val, labels={'x': 'Categories', 'y': 'Values'},
                            title='Nombre de Livres par Catégorie', color=categ)
        return dcc.Graph(figure=bar_chart)
    

    elif selected_tab == 'search_book':
        # Requête 3: Rechercher un livre
        return html.Div([
            dcc.Input(id='search-input', type='text', placeholder='Entrez le titre du livre...'),
            html.Button(id='search-button', n_clicks=0, children='Rechercher'),
            html.Div(id='search-result')
        ])

# Callback pour la recherche de livre
@app.callback(
    Output('search-result', 'children'),
    [Input('search-button', 'n_clicks')],
    [dash.dependencies.State('search-input', 'value')]
)
def search_book(n_clicks, search_input):
    if n_clicks > 0:
        item = chercher_livre(search_input)
        return f"Vous avez recherché le livre : {search_input} {item}"
    
# Exécution de l'application Dash
if __name__ == '__main__':
    app.run_server()



