import re

query = "CREATE TABLE etudiants (id INT, nom TEXT);"
pattern = r'CREATE\s+TABLE\s+\w+\s*\((.*)\);'

match = re.match(pattern, query, re.IGNORECASE)

if match:
    print("Match complet :", match.group())
    print("Nom de table  :", match.group(1))
    print("Colonnes      :", match.group(2))
