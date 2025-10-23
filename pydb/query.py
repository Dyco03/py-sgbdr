# query.py
import re
import shlex

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
        """Exécute une commande UPDATE."""
        table_name = match.group(1)
        set_clause = match.group(2)
        where_clause = match.group(3)
        
        try:
            table = self.database.get_table(table_name)
            
            # Parser la clause SET
            updates = {}
            for item in self._split_args(set_clause):
                parts = item.split('=', 1)
                if len(parts) != 2:
                    return {"error": f"Clause SET invalide: {item}"}
                
                col_name = parts[0].strip()
                value = self._parse_value(parts[1].strip())
                updates[col_name] = value
            
            # Parser la clause WHERE
            conditions = self._parse_conditions(where_clause)
            
            # Récupérer les enregistrements à mettre à jour
            records = table.select(conditions)
            
            if not records:
                return {"error": "Aucun enregistrement trouvé correspondant aux conditions"}
            
            # Mettre à jour chaque enregistrement
            for record in records:
                table.update(record[table.primary_key], updates)
            
            return {"success": f"{len(records)} enregistrement(s) mis à jour"}
        except Exception as e:
            return {"error": str(e)}
    
    def _execute_delete(self, match):
        """Exécute une commande DELETE FROM."""
        table_name = match.group(1)
        where_clause = match.group(2)
        
        try:
            table = self.database.get_table(table_name)
            
            # Si pas de clause WHERE, supprimer tous les enregistrements
            if not where_clause:
                count = len(table.data)
                table.data = []
                table.indexes = {table.primary_key: {}}
                table._save()
                return {"success": f"{count} enregistrement(s) supprimé(s)"}
            
            # Parser la clause WHERE
            conditions = self._parse_conditions(where_clause)
            
            # Récupérer les enregistrements à supprimer
            records = table.select(conditions)
            
            if not records:
                return {"error": "Aucun enregistrement trouvé correspondant aux conditions"}
            
            # Supprimer chaque enregistrement
            for record in records:
                table.delete(record[table.primary_key])
            
            return {"success": f"{len(records)} enregistrement(s) supprimé(s)"}
        except Exception as e:
            return {"error": str(e)}
    
    def _execute_select(self, match):
        """Exécute une commande SELECT."""
        fields_str = match.group(1)
        table_name = match.group(2)
        where_clause = match.group(3)
        order_by = match.group(4)
        limit = match.group(5)
        
        try:
            table = self.database.get_table(table_name)
            
            # Parser les champs à sélectionner
            if fields_str.strip() == '*':
                fields = None  # Tous les champs
            else:
                fields = [field.strip() for field in fields_str.split(',')]
            
            # Parser la clause WHERE
            conditions = self._parse_conditions(where_clause) if where_clause else None
            
            # Exécuter la requête SELECT
            results = table.select(conditions, fields)
            
            # Appliquer ORDER BY si présent
            if order_by:
                field, *direction = order_by.strip().split()
                reverse = direction and direction[0].upper() == 'DESC'
                results.sort(key=lambda x: x.get(field), reverse=reverse)
            
            # Appliquer LIMIT si présent
            if limit:
                results = results[:int(limit)]
            
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