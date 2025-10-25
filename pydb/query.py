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
        
        create_relation_match = re.match(r'CREATE\s+RELATION\s+(\w+)\s+BETWEEN\s+(\w+)\((\w+)\)\s+AND\s+(\w+)\((\w+)\)\s+TYPE\s+(\w+);', query_string, re.IGNORECASE)
        if create_relation_match:
            return self._execute_create_relation(create_relation_match)
        
        link_match = re.match(r'LINK\s+(\w+)\s+\((\w+)=(\w+)\)\s+TO\s+(\w+)\s+\((\w+)=(\w+)\);', query_string, re.IGNORECASE)
        if link_match:
            return self._execute_link(link_match)
        
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

            # ---- Parser la clause SET ----
            updates = {}
            for item in set_clause.split(','):
                parts = item.split('=', 1)
                if len(parts) != 2:
                    return {"error": f"Clause SET invalide: {item}"}
                col_name = parts[0].strip()
                value = self._parse_value(parts[1].strip())
                updates[col_name] = value

            # ---- Construire le masque WHERE ----
            if where_clause:
                query_str = where_clause.replace("AND", "and").replace("OR", "or").replace("=", "==")
                mask = df.eval(query_str)  # masque booléen pandas
            else:
                mask = pd.Series([True] * len(df))  # cree une serie de true pour toutes les lignes

            # Vérifier s’il y a des lignes correspondantes
            if not mask.any():
                return {"error": "Aucun enregistrement trouvé correspondant aux conditions"}

            # ---- Appliquer les mises à jour ----
            for col, val in updates.items():
                df.loc[mask, col] = val

            # ---- Écrire les changements dans la table originale ----
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

            # ---- Si pas de WHERE  tout supprimer ----
            if not where_clause:
                count = len(df)
                table.data = []  # vider complètement
                table.indexes = {table.primary_key: {}}
                table._save()
                return {"success": f"{count} enregistrement(s) supprimé(s)"}

            # ---- Construire le masque WHERE ----
            query_str = where_clause.replace("AND", "and").replace("OR", "or").replace('=', '==')
            mask = df.eval(query_str)  # lignes à supprimer

            if not mask.any():
                return {"error": "Aucun enregistrement trouvé correspondant aux conditions"}

            # ---- Supprimer les lignes correspondantes ----
            df = df[~mask]  # supprime les lignes ou masque = true

            # ---- Réécrire les données dans la table ----
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

            # Convertir le résultat en liste de dictionnaires
            results = df.to_dict(orient='records')

            return {"data": results, "count": len(results)}

        except Exception as e:
            return {"error": str(e)}

    
    def _execute_create_relation(self, match):
        """Exécute une commande CREATE RELATION."""
        relation_name = match.group(1)
        source_table_name = match.group(2)
        source_field = match.group(3)
        target_table_name = match.group(4)
        target_field = match.group(5)
        relation_type = match.group(6).lower()
        # Niharison Dyco Miasa will become the best programmer in the world
        try:
            # Récupérer les tables
            source_table = self.database.get_table(source_table_name)
            target_table = self.database.get_table(target_table_name)
            
            # Valider le type de relation
            valid_types = ['one_to_one', 'one_to_many', 'many_to_many']
            if relation_type not in valid_types:
                return {"error": f"Type de relation non valide. Utilisez: {', '.join(valid_types)}"}
            
            # Créer la relation
            from .relation import Relation
            relation = Relation(
                source_table=source_table,
                target_table=target_table,
                relation_type=relation_type,
                source_field=source_field,
                target_field=target_field
            )
            
            # Stocker la relation dans les métadonnées
            if not hasattr(self.database, 'relations'):
                self.database.relations = {}
            
            self.database.relations[relation_name] = relation
            
            return {"success": f"Relation '{relation_name}' créée avec succès"}
        except Exception as e:
            return {"error": str(e)}
    
    def _execute_link(self, match):
        """Exécute une commande LINK pour lier deux enregistrements."""
        source_table_name = match.group(1)
        source_field = match.group(2)
        source_value = match.group(3)
        target_table_name = match.group(4)
        target_field = match.group(5)
        target_value = match.group(6)
        
        try:
            # Récupérer les tables
            source_table = self.database.get_table(source_table_name)
            target_table = self.database.get_table(target_table_name)
            
            # Trouver les relations correspondantes
            relation = None
            if hasattr(self.database, 'relations'):
                for rel in self.database.relations.values():
                    if (rel.source_table.name == source_table_name and 
                        rel.target_table.name == target_table_name):
                        relation = rel
                        break
            
            if not relation:
                return {"error": "Aucune relation trouvée entre ces tables"}
            
            # Récupérer les enregistrements
            source_records = source_table.select({source_field: self._parse_value(source_value)})
            target_records = target_table.select({target_field: self._parse_value(target_value)})
            
            if not source_records:
                return {"error": f"Aucun enregistrement trouvé dans {source_table_name} avec {source_field}={source_value}"}
            
            if not target_records:
                return {"error": f"Aucun enregistrement trouvé dans {target_table_name} avec {target_field}={target_value}"}
            
            # Lier les enregistrements
            relation.link(source_records[0], target_records[0])
            
            return {"success": "Enregistrements liés avec succès"}
        except Exception as e:
            return {"error": str(e)}
    
    def _parse_conditions(self, where_clause):
        """Parse une clause WHERE en dictionnaire de conditions."""
        if not where_clause:
            return {}
        
        conditions = {}
        parts = where_clause.split(' AND ')
        
        for part in parts:
            if '=' not in part:
                continue
            
            field, value = part.split('=', 1)
            field = field.strip()
            value = self._parse_value(value.strip())
            conditions[field] = value
        
        return conditions
    
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
    
    def _split_args(self, args_str):
        """Divise une chaîne d'arguments en respectant les parenthèses et les guillemets."""
        if not args_str:
            return []
        
        try:
            return shlex.split(args_str.replace(',', ' , ').replace('(', ' ( ').replace(')', ' ) '))
        except ValueError:
            # Fallback simple si shlex échoue
            return [arg.strip() for arg in args_str.split(',')]
        
    #function pour le join
    def _execute_select_join(self, match):
        """Exécute une commande SELECT avec JOIN."""
        fields_str = match.group(1)
        table_name = match.group(2)
        join_clause = match.group(3)
        where_clause = match.group(4)
        order_by = match.group(5)
        limit = match.group(6)
        
        try:
            # Récupérer la table principale
            main_table = self.database.get_table(table_name)
            
            # Parser la clause JOIN
            join_pattern = r'(INNER|LEFT|RIGHT)\s+JOIN\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
            join_matches = re.finditer(join_pattern, join_clause, re.IGNORECASE)
            
            joins = []
            for join_match in join_matches:
                join_type = join_match.group(1).upper()
                join_table_name = join_match.group(2)
                left_table = join_match.group(3)
                left_field = join_match.group(4)
                right_table = join_match.group(5)
                right_field = join_match.group(6)
                
                join_table = self.database.get_table(join_table_name)
                joins.append({
                    'type': join_type,
                    'table': join_table,
                    'table_name': join_table_name,
                    'left_table': left_table,
                    'left_field': left_field,
                    'right_table': right_table,
                    'right_field': right_field
                })
            
            # Exécuter le JOIN
            results = self._perform_join(main_table, table_name, joins)
            
            # Appliquer WHERE si présent
            if where_clause:
                conditions = self._parse_conditions(where_clause)
                filtered_results = []
                for result in results:
                    match = True
                    for field, value in conditions.items():
                        # Gérer les champs avec préfixe table.field
                        if '.' in field:
                            if field not in result or result[field] != value:
                                match = False
                                break
                        else:
                            # Chercher dans tous les champs possibles
                            found = False
                            for key in result.keys():
                                if key.endswith('.' + field) and result[key] == value:
                                    found = True
                                    break
                            if not found:
                                match = False
                                break
                    
                    if match:
                        filtered_results.append(result)
                
                results = filtered_results
            
            # Sélectionner les champs demandés
            if fields_str.strip() != '*':
                fields = [field.strip() for field in fields_str.split(',')]
                selected_results = []
                for result in results:
                    selected_record = {}
                    for field in fields:
                        # Gérer les champs avec ou sans préfixe table.field
                        if '.' in field:
                            if field in result:
                                selected_record[field] = result[field]
                        else:
                            # Chercher dans tous les champs possibles
                            for key in result.keys():
                                if key.endswith('.' + field):
                                    selected_record[field] = result[key]
                                    break
                    selected_results.append(selected_record)
                results = selected_results
            
            # Appliquer ORDER BY si présent
            if order_by:
                field, *direction = order_by.strip().split()
                reverse = direction and direction[0].upper() == 'DESC'
                # Gérer les champs avec ou sans préfixe
                if '.' not in field:
                    # Chercher le champ avec préfixe
                    for key in results[0].keys() if results else []:
                        if key.endswith('.' + field):
                            field = key
                            break
                results.sort(key=lambda x: x.get(field), reverse=reverse)
            
            # Appliquer LIMIT si présent
            if limit:
                results = results[:int(limit)]
            
            return {"data": results, "count": len(results)}
        except Exception as e:
            return {"error": str(e)}
    
    def _perform_join(self, main_table, main_table_name, joins):
        """Effectue les jointures entre tables."""
        # Commencer avec tous les enregistrements de la table principale
        results = []
        for main_record in main_table.data:
            # Préfixer les champs avec le nom de la table
            prefixed_main = {f"{main_table_name}.{k}": v for k, v in main_record.items()}
            
            # Pour chaque jointure
            current_results = [prefixed_main]
            
            for join in joins:
                new_results = []
                
                for current_record in current_results:
                    # Déterminer quelle table est la gauche et la droite
                    if join['left_table'] == main_table_name:
                        left_key = f"{main_table_name}.{join['left_field']}"
                        left_value = current_record.get(left_key)
                        
                        # Chercher les enregistrements correspondants dans la table de jointure
                        matching_records = [r for r in join['table'].data 
                                          if r.get(join['right_field']) == left_value]
                        
                    else:
                        # La table de jointure est à gauche
                        right_key = f"{join['table_name']}.{join['right_field']}"
                        
                        # Trouver la valeur à matcher depuis current_record
                        left_value = None
                        for key, value in current_record.items():
                            if key.endswith('.' + join['left_field']):
                                left_value = value
                                break
                        
                        # Chercher les enregistrements correspondants
                        matching_records = [r for r in join['table'].data 
                                          if r.get(join['right_field']) == left_value]
                    
                    # Appliquer le type de jointure
                    if join['type'] == 'INNER':
                        # INNER JOIN: ne garder que si match trouvé
                        for match_record in matching_records:
                            merged_record = current_record.copy()
                            # Préfixer les champs de la table jointe
                            for k, v in match_record.items():
                                merged_record[f"{join['table_name']}.{k}"] = v
                            new_results.append(merged_record)
                    
                    elif join['type'] == 'LEFT':
                        # LEFT JOIN: garder l'enregistrement même sans match
                        if matching_records:
                            for match_record in matching_records:
                                merged_record = current_record.copy()
                                for k, v in match_record.items():
                                    merged_record[f"{join['table_name']}.{k}"] = v
                                new_results.append(merged_record)
                        else:
                            # Ajouter NULL pour les champs de la table jointe
                            merged_record = current_record.copy()
                            for field in join['table'].schema.keys():
                                merged_record[f"{join['table_name']}.{field}"] = None
                            new_results.append(merged_record)
                    
                    elif join['type'] == 'RIGHT':
                        # RIGHT JOIN: pour chaque enregistrement de la table de droite
                        if not matching_records:
                            # Si pas de match, créer un enregistrement avec NULL à gauche
                            for right_record in join['table'].data:
                                merged_record = {}
                                # NULL pour la table de gauche
                                for field in main_table.schema.keys():
                                    merged_record[f"{main_table_name}.{field}"] = None
                                # Valeurs de la table de droite
                                for k, v in right_record.items():
                                    merged_record[f"{join['table_name']}.{k}"] = v
                                new_results.append(merged_record)
                        else:
                            for match_record in matching_records:
                                merged_record = current_record.copy()
                                for k, v in match_record.items():
                                    merged_record[f"{join['table_name']}.{k}"] = v
                                new_results.append(merged_record)
                
                current_results = new_results
            
            results.extend(current_results)
        
        return results