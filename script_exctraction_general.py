"""
fais appel a tous les code d'extraction 
"""

import subprocess
import sys

# Liste des fichiers Python à exécuter
scripts = [
    "script_extraction_api_rest.py",
    "script_extraction_big_data.py",
    # Ajoute d'autres scripts ici si nécessaire
]

# Exécution de chaque fichier Python
for script in scripts:
    print(f"Exécution de {script}...")
    
    try:
        # Utilise subprocess pour exécuter chaque script
        result = subprocess.run(['python3', script], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de l'exécution de {script} : {e.stderr.decode()}")
        sys.exit(1)

print("Tous les scripts ont été exécutés avec succès.")