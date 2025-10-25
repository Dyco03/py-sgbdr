# Créer et utiliser une base de données
PyDB> create_db my_database
PyDB> use my_database

# Créer des tables
PyDB (my_database)> CREATE TABLE utilisateurs (id INT PRIMARY KEY, nom TEXT, email TEXT, age INT);
PyDB (my_database)> CREATE TABLE articles (id INT PRIMARY KEY, titre TEXT, contenu TEXT, auteur_id INT);

# Insérer des données
PyDB (my_database)> INSERT INTO utilisateurs (nom, email, age) VALUES ('Jean Dupont', 'jean@example.com', 32);
PyDB (my_database)> INSERT INTO utilisateurs (nom, email, age) VALUES ('Marie Martin', 'marie@example.com', 28);
PyDB (my_database)> INSERT INTO articles (titre, contenu, auteur_id) VALUES ('Premier article', 'Contenu du premier article', 1);

# Sélectionner des données
PyDB (my_database)> SELECT * FROM utilisateurs;
PyDB (my_database)> SELECT nom, email FROM utilisateurs WHERE age > 30;

# Mettre à jour des données
PyDB (my_database)> UPDATE utilisateurs SET age = 33 WHERE id = 1;

# Supprimer des données
PyDB (my_database)> DELETE FROM articles WHERE auteur_id = 2;

# Créer des relations
PyDB (my_database)> CREATE RELATION auteur_articles BETWEEN utilisateurs(id) AND articles(auteur_id) TYPE one_to_many;

# Lier des enregistrements
PyDB (my_database)> LINK utilisateurs (id=1) TO articles (id=1);

# Afficher la structure d'une table
PyDB (my_database)> describe utilisateurs

------------------------------------------------------------
Ce SGBDR Python offre:

Création et gestion de bases de données et tables
Opérations CRUD (CREATE, READ, UPDATE, DELETE)
Différents types de données (int, str, float, datetime)
Relations entre tables (one_to_one, one_to_many, many_to_many)
Requêtes avec conditions WHERE
Sauvegarde et restauration des données
Interface en ligne de commande similaire à SQL

Fonctionnalités ajoutées :

INNER JOIN : Retourne seulement les lignes qui ont des correspondances dans les deux tables
LEFT JOIN : Retourne toutes les lignes de la table de gauche, avec NULL pour les non-correspondances à droite
RIGHT JOIN : Retourne toutes les lignes de la table de droite, avec NULL pour les non-correspondances à gauche

[
    SELECT * FROM table1 INNER JOIN table2 ON table1.champ1 = table2.champ2;
    SELECT * FROM table1 LEFT JOIN table2 ON table1.champ1 = table2.champ2;
    SELECT * FROM table1 RIGHT JOIN table2 ON table1.champ1 = table2.champ2;
]

Fonctionnalités supportées avec JOIN

SELECT avec champs spécifiques : SELECT table1.nom, table2.titre FROM ...
WHERE : Filtrer les résultats après la jointure
ORDER BY : Trier les résultats
LIMIT : Limiter le nombre de résultats


Limitations et améliorations possibles

Ce système est une implémentation de base qui pourrait être améliorée avec:

Plus d'opérateurs dans les clauses WHERE (>, <, !=, LIKE, etc.)
Jointures entre tables
Index pour améliorer les performances
Transactions et gestion de la concurrence
Meilleure gestion des types de données
Interface graphique

Avec cette architecture modulaire, vous pouvez facilement étendre le système selon vos besoins, tout en conservant une syntaxe proche de SQL pour les requêtes.