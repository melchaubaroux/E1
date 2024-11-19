#!/bin/bash

# Liste des fichiers Python à exécuter
scripts=(
    "source1.py"
    "source2.py"
    "source3.py"
    # Ajoute d'autres scripts ici si nécessaire
)

# Exécution de chaque fichier Python
for script in "${scripts[@]}"
do
    echo "Exécution de $script..."
    python3 "$script"
    
    # Vérifie si l'exécution a échoué
    if [ $? -ne 0 ]; then
        echo "Erreur lors de l'exécution de $script. Arrêt du processus."
        exit 1
    fi
done

echo "Tous les scripts ont été exécutés avec succès."
