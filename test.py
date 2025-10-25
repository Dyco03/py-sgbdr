import pandas as pd

# Création d’un petit tableau
data = {
    "Nom": ["Alice", "Bob", "Claire"],
    "Âge": [25, 30, 27],
    "Ville": ["Paris", "Lyon", "Marseille"]
}

df = pd.DataFrame(data)
print(df.sort_values(by="nom"))
