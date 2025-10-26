# query.py
import re
import shlex
import pandas as pd

class QueryParser:
    def __init__(self, database):
        self.database = database
    
    def parse_and_execute(self, query_string):
        """
        Parse et exécute une requête SQL-like.
        
        Args:
            query_string (str): Requête à exécuter
        
        Returns:
            mixed: Résultat de l'exécution de la requête
        """
        # Diviser la requête en tokens
        query_string = query_string.strip()
        if not query_string.endswith(';'):
            query_string += ';'
        
        # Identifier le type de commande
        create_table_match = re.match(r'CREATE\s+TABLE\s+(\w+)\s*\((.*)\);', query_string, re.IGNORECASE)
        if create_table_match:
            return self._execute_create_table(create_table_match)
        
        drop_table_match = re.match(r'DROP\s+TABLE\s+(\w+);', query_string, re.IGNORECASE)
        if drop_table_match:
            return self._execute_drop_table(drop_table_match)
        
        insert_match = re.match(r'INSERT\s+INTO\s+(\w+)\s*(\((.*?)\))?\s*VALUES\s*\((.*?)\);', query_string, re.IGNORECASE)
        if insert_match:
            return self._execute_insert(insert_match)
        
        update_match = re.match(r'UPDATE\s+(\w+)\s+SET\s+(.*?)\s+WHERE\s+(.*?);', query_string, re.IGNORECASE)
        if update_match:
            return self._execute_update(update_match)
        
        delete_match = re.match(r'DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*?))?;', query_string, re.IGNORECASE)
        if delete_match:
            return self._execute_delete(delete_match)
        
        select_match = re.match(r'SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*?))?(?:\s+ORDER\s+BY\s+(.*?))?(?:\s+LIMIT\s+(\d+))?;', query_string, re.IGNORECASE)
        if select_match:
            return self._execute_select(select_match)

        # Pattern pour SELECT avec JOIN
        select_join_match = re.match(
            r'SELECT\s+(.*?)\s+FROM\s+(\w+)\s+((?:INNER|LEFT|RIGHT)\s+JOIN\s+\w+\s+ON\s+.*?)(?:\s+WHERE\s+(.*?))?(?:\s+ORDER\s+BY\s+(.*?))?(?:\s+LIMIT\s+(\d+))?;',
            query_string, 
            re.IGNORECASE
        )
        if select_join_match:
            return self._execute_select_join(select_join_match)
        
        # Commande non reconnue
        return {"error": f"Commande non reconnue: {query_string}"}
    
    def _execute_create_table(self, match):
        """Exécute une commande CREATE TABLE."""
        table_name = match.group(1)
        columns_def = match.group(2)
        
        # Parser les définitions de colonnes
        schema = {}
        primary_key = None
        
        for column_def in columns_def.split(','):
            parts = column_def.strip().split()
            if len(parts) < 2:
                return {"error": f"Définition de colonne invalide: {columns_def}"}
            
            col_name = parts[0]
            col_type = parts[1].lower()
            
            # Mapper les types SQL aux types Python
            if col_type == 'int' or col_type == 'integer':
                schema[col_name] = 'int'
            elif col_type == 'text' or col_type == 'varchar':
                schema[col_name] = 'str'
            elif col_type == 'float' or col_type == 'real':
                schema[col_name] = 'float'
            elif col_type == 'date' or col_type == 'datetime':
                schema[col_name] = 'datetime'
            else:
                return {"error": f"Type de données non pris en charge: {col_type}"}
            
            # Vérifier si c'est une clé primaire
            if 'primary key' in column_def.lower():
                primary_key = col_name
        
        try:
            # Créer la table
            if primary_key:
                table = self.database.create_table(table_name, schema)
                table.primary_key = primary_key
            else:
                table = self.database.create_table(table_name, schema)
            
            return {"success": f"Table '{table_name}' créée avec succès"}
        except Exception as e:
            return {"error": str(e)}
    
    def _execute_drop_table(self, match):
        """Exécute une commande DROP TABLE."""
        table_name = match.group(1)
        
        try:
            self.database.drop_table(table_name)
            return {"success": f"Table '{table_name}' supprimée avec succès"}
        except Exception as e:
            return {"error": str(e)}
    
    def _execute_insert(self, match):
        """Exécute une commande INSERT INTO."""
        table_name = match.group(1)
        columns_str = match.group(3)
        values_str = match.group(4)
        
        
        try:
            table = self.database.get_table(table_name)
            
            # Parser les colonnes et les valeurs
            if columns_str:
                columns = [col.strip() for col in columns_str.split(',')]
            else:
                columns = table.schema.keys()
            
            values = values_str.split(',')
            
            
            
            if len(columns) != len(values):
                return {"error": f"Le nombre de colonnes {columns} et de valeurs {values} ne correspond pas"}
            
            # Créer le dictionnaire d'enregistrement
            record = dict(zip(columns, values))
            #ceci est un test à supprimer
            #return {"error": f"le record est {record}"}
            # Insérer l'enregistrement
            pk = table.insert(record)
            return {"success": f"Enregistrement inséré avec l'ID {pk}"}
        except Exception as e:
            return {"error": str(e)}

    def _execute_update(self, match):
        """Exécute une commande UPDATE en utilisant pandas."""
        table_name = match.group(1)
        set_clause = match.group(2)
        where_clause = match.group(3)

        try:
            # Charger la table dans un DataFrame
            table = self.database.get_table(table_name)
            df = pd.DataFrame(table.data)  # table.data = liste de dicts

            #  Parser la clause SET
            updates = {}
            for item in set_clause.split(','):
                parts = item.split('=', 1)
                if len(parts) != 2:
                    return {"error": f"Clause SET invalide: {item}"}
                col_name = parts[0].strip()
                value = self._parse_value(parts[1].strip())
                updates[col_name] = value

            #  Construire le masque WHERE 
            if where_clause:
                query_str = where_clause.replace("AND", "and").replace("OR", "or").replace("=", "==")
                mask = df.eval(query_str)  # masque booléen pandas
            else:
                mask = pd.Series([True] * len(df))  # cree une serie de true pour toutes les lignes

            # Vérifier s’il y a des lignes correspondantes
            if not mask.any():
                return {"error": "Aucun enregistrement trouvé correspondant aux conditions"}

            #  Appliquer les mises à jour 
            for col, val in updates.items():
                df.loc[mask, col] = val

            #  Écrire les changements dans la table originale 
            table.data = df.to_dict(orient="records")

            return {"success": f"{mask.sum()} enregistrement(s) mis à jour"}

        except Exception as e:
            return {"error": str(e)}

    
    def _execute_delete(self, match):
        """Exécute une commande DELETE FROM en utilisant pandas."""
        table_name = match.group(1)
        where_clause = match.group(2)

        try:
            # Charger la table dans un DataFrame
            table = self.database.get_table(table_name)
            df = pd.DataFrame(table.data)

            # Si la table est vide
            if df.empty:
                return {"error": "Aucune donnée à supprimer"}

            #  Si pas de WHERE  tout supprimer 
            if not where_clause:
                count = len(df)
                table.data = []  # vider complètement
                table.indexes = {table.primary_key: {}}
                table._save()
                return {"success": f"{count} enregistrement(s) supprimé(s)"}

            #  Construire le masque WHERE 
            query_str = where_clause.replace("AND", "and").replace("OR", "or").replace('=', '==')
            mask = df.eval(query_str)  # lignes à supprimer

            if not mask.any():
                return {"error": "Aucun enregistrement trouvé correspondant aux conditions"}

            #  Supprimer les lignes correspondantes 
            df = df[~mask]  # supprime les lignes ou masque = true

            #  Réécrire les données dans la table 
            table.data = df.to_dict(orient="records")
            table._save()

            return {"success": f"{mask.sum()} enregistrement(s) supprimé(s)"}

        except Exception as e:
            return {"error": str(e)}

    
    def _execute_select(self, match):
        """Exécute une commande SELECT en utilisant pandas."""
        fields_str = match.group(1)
        table_name = match.group(2)
        where_clause = match.group(3)
        order_by = match.group(4)
        limit = match.group(5)

        try:
            # Récupérer la table et la transformer en DataFrame
            table = self.database.get_table(table_name)
            df = pd.DataFrame(table.data) 

            # Appliquer WHERE avec pandas
            if where_clause:
                # Remplacer AND/OR par leurs équivalents (query() les comprend)
                query_str = where_clause.replace('AND', 'and').replace('OR', 'or').replace('=', '==')
                df = df.query(query_str)

            # Sélectionner les champs demandés
            if fields_str.strip() != '*':
                fields = [f.strip() for f in fields_str.split(',')]
                for field in fields:
                    if field not in table.schema:
                        raise ValueError(f"Le champ '{field}' n'existe pas dans le schéma")
                    
                df = df[fields]

            # Appliquer ORDER BY si présent
            if order_by:
                parts = order_by.strip().split()
                field = parts[0]
                ascending = not (len(parts) > 1 and parts[1].upper() == 'DESC')
                df = df.sort_values(by=field, ascending=ascending)

            # Appliquer LIMIT si présent
            if limit:
                df = df.head(int(limit))

            return {"data": df}

        except Exception as e:
            return {"error": str(e)}
    
    def _parse_values(self, values_str):
        """Parse une chaîne de valeurs séparées par des virgules."""
        values = []
        for arg in self._split_args(values_str):
            values.append(self._parse_value(arg))
        return values
    
    def _parse_value(self, value_str):
        """Convertit une chaîne en valeur typée."""
        value_str = value_str.strip()
        
        # NULL
        if value_str.upper() == 'NULL':
            return None
        
        # Nombre entier
        if value_str.isdigit():
            return int(value_str)
        
        # Nombre à virgule
        try:
            return float(value_str)
        except ValueError:
            pass
        
        # Chaîne de caractères (enlever les guillemets)
        if (value_str.startswith('"') and value_str.endswith('"')) or \
           (value_str.startswith("'") and value_str.endswith("'")):
            return value_str[1:-1]
        
        return value_str
        
    def _execute_select_join(self, match):
        """Exécute une commande SELECT avec JOIN (version simplifiée avec pandas)."""
        fields_str = match.group(1)
        table_name = match.group(2)
        join_clause = match.group(3)
        where_clause = match.group(4)
        order_by = match.group(5)
        limit = match.group(6)

        try:
            # Table principale
            main_table = self.database.get_table(table_name)
            df = pd.DataFrame(main_table.data)

            # Détecter la jointure
            join_pattern = r'(INNER|LEFT|RIGHT)\s+JOIN\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
            join_match = re.search(join_pattern, join_clause, re.IGNORECASE)
            if not join_match:
                return {"error": "Syntaxe JOIN invalide"}

            join_type = join_match.group(1).upper()
            join_table_name = join_match.group(2)
            left_table = join_match.group(3)
            left_field = join_match.group(4)
            right_table = join_match.group(5)
            right_field = join_match.group(6)

            # Charger la table à joindre
            join_table = self.database.get_table(join_table_name)
            df_join = pd.DataFrame(join_table.data)

            # Traduire le type de JOIN pour pandas
            join_types = {"INNER": "inner", "LEFT": "left", "RIGHT": "right"}
            how = join_types.get(join_type, "inner")

            # Exécuter le JOIN avec pandas
            df = pd.merge(
                df,
                df_join,
                left_on=left_field,
                right_on=right_field,
                how=how,
                suffixes=(f"_{table_name}", f"_{join_table_name}")
            )

            # Appliquer WHERE si présent
            if where_clause:
                # Très basique (ex: "age = 30")
                conditions = self._parse_conditions(where_clause)
                for field, value in conditions.items():
                    # Retirer guillemets autour de value si présents
                    if isinstance(value, str):
                        value = value.strip("'\"")
                    if field in df.columns:
                        df = df[df[field] == value]

            # Sélectionner les colonnes
            if fields_str.strip() != "*":
                fields = [f.strip() for f in fields_str.split(",")]
                df = df[fields]

            # ORDER BY
            if order_by:
                parts = order_by.strip().split()
                field = parts[0]
                ascending = not (len(parts) > 1 and parts[1].upper() == "DESC")
                if field in df.columns:
                    df = df.sort_values(by=field, ascending=ascending)

            # LIMIT
            if limit:
                df = df.head(int(limit))

            # Retourner les résultats
            return {"data": df}

        except Exception as e:
            return {"error": str(e)}
