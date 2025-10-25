# table.py
import os
import pickle
from datetime import datetime

class Table:
    def __init__(self, name, schema, database, primary_key="id"):
        self.name = name
        self.schema = schema
        self.database = database
        self.primary_key = primary_key
        self.data = []
        self.indexes = {primary_key: {}}  # Index sur clé primaire
        
        # Vérifier et ajouter automatiquement une clé primaire si nécessaire
        if primary_key not in schema:
            self.schema[primary_key] = "int"
        
        # Charger les données si la table existe
        self._load()
    
    @classmethod
    def from_metadata(cls, metadata, database):
        """Crée une table à partir de métadonnées."""
        table = cls(
            name=metadata["name"],
            schema=metadata["schema"],
            database=database,
            primary_key=metadata["primary_key"]
        )
        return table
    
    def get_metadata(self):
        """Renvoie les métadonnées de la table."""
        return {
            "name": self.name,
            "schema": self.schema,
            "primary_key": self.primary_key
        }
    
    def insert(self, record):
        """
        Insère un enregistrement dans la table.
        
        Args:
            record (dict): Enregistrement à insérer
        
        Returns:
            int: ID de l'enregistrement inséré
        """
        # prevenir les erreurs de type
        record = self.real_type(record)
        #return {"error":f"l'erreur est {record}"}
        # Valider le schéma
        for field, value in record.items():
            if field not in self.schema:
                raise ValueError(f"Le champ '{field}' n'existe pas dans le schéma")
            
            # Vérification basique des types
            expected_type = self.schema[field]
            if expected_type == "int" and not isinstance(value, int):
                raise TypeError(f"Le champ '{field}' doit être de type int")
            elif expected_type == "str" and not isinstance(value, str):
                raise TypeError(f"Le champ '{field}' doit être de type str")
            elif expected_type == "float" and not isinstance(value, (int, float)):
                raise TypeError(f"Le champ '{field}' doit être de type float")
            elif expected_type == "datetime" and not isinstance(value, datetime):
                raise TypeError(f"Le champ '{field}' doit être de type datetime")
        
        # Gérer les champs manquants
        for field in self.schema:
            if field not in record and field != self.primary_key:
                record[field] = None
        
        # Gérer la clé primaire
        if self.primary_key not in record:
            pk_value = len(self.data) + 1
            record[self.primary_key] = pk_value
        else:
            pk_value = record[self.primary_key]
            # Vérifier l'unicité de la clé primaire
            if pk_value in self.indexes[self.primary_key]:
                raise ValueError(f"La valeur '{pk_value}' existe déjà pour la clé primaire")
        
        # Ajouter l'enregistrement et mettre à jour l'index
        self.data.append(record)
        self.indexes[self.primary_key][pk_value] = len(self.data) - 1
        
        # Sauvegarder la table
        self._save()
        
        return pk_value
    
    def update(self, primary_key_value, updates):
        """
        Met à jour un enregistrement.
        
        Args:
            primary_key_value: Valeur de la clé primaire
            updates (dict): Dictionnaire des champs à mettre à jour
        
        Returns:
            bool: True si la mise à jour a réussi
        """
        if primary_key_value not in self.indexes[self.primary_key]:
            raise ValueError(f"Aucun enregistrement avec {self.primary_key}={primary_key_value}")
        
        record_index = self.indexes[self.primary_key][primary_key_value]
        record = self.data[record_index]
        
        # Valider et mettre à jour les champs
        for field, value in updates.items():
            if field == self.primary_key:
                raise ValueError("La clé primaire ne peut pas être modifiée")
            
            if field not in self.schema:
                raise ValueError(f"Le champ '{field}' n'existe pas dans le schéma")
            
            # Vérification basique des types
            expected_type = self.schema[field]
            if expected_type == "int" and not isinstance(value, int):
                raise TypeError(f"Le champ '{field}' doit être de type int")
            elif expected_type == "str" and not isinstance(value, str):
                raise TypeError(f"Le champ '{field}' doit être de type str")
            elif expected_type == "float" and not isinstance(value, (int, float)):
                raise TypeError(f"Le champ '{field}' doit être de type float")
            elif expected_type == "datetime" and not isinstance(value, datetime):
                raise TypeError(f"Le champ '{field}' doit être de type datetime")
            
            record[field] = value
        
        # Sauvegarder la table
        self._save()
        
        return True
    
    def delete(self, primary_key_value):
        """
        Supprime un enregistrement.
        
        Args:
            primary_key_value: Valeur de la clé primaire
        
        Returns:
            bool: True si la suppression a réussi
        """
        if primary_key_value not in self.indexes[self.primary_key]:
            raise ValueError(f"Aucun enregistrement avec {self.primary_key}={primary_key_value}")
        
        record_index = self.indexes[self.primary_key][primary_key_value]
        
        # Supprimer l'enregistrement et l'index
        del self.data[record_index]
        del self.indexes[self.primary_key][primary_key_value]
        
        # Mettre à jour les indexes (décaler les index des enregistrements suivants)
        self.indexes[self.primary_key] = {pk: idx if idx < record_index else idx - 1 
                                         for pk, idx in self.indexes[self.primary_key].items()}
        
        # Sauvegarder la table
        self._save()
        
        return True
    
    def select(self, conditions=None, fields=None):
        """
        Sélectionne des enregistrements selon des conditions.
        
        Args:
            conditions (dict, optional): Conditions de filtrage {champ: valeur}
            fields (list, optional): Champs à retourner
        
        Returns:
            list: Liste des enregistrements correspondants
        """
        results = []
        
        for record in self.data:
            # Appliquer les conditions
            if conditions:
                match = True
                for field, value in conditions.items():
                    if field not in record or record[field] != value:
                        match = False
                        break
                
                if not match:
                    continue
            
            # Filtrer les champs si nécessaire
            if fields:
                filtered_record = {field: record[field] for field in fields if field in record}
                results.append(filtered_record)
            else:
                results.append(record.copy())
        
        return results
    
    def _save(self):
        """Sauvegarde les données de la table."""
        table_file = os.path.join(self.database.storage_dir, f"{self.name}.data")
        with open(table_file, "wb") as f:
            pickle.dump({
                "data": self.data,
                "indexes": self.indexes
            }, f)
    
    def _load(self):
        """Charge les données de la table."""
        table_file = os.path.join(self.database.storage_dir, f"{self.name}.data")
        if not os.path.exists(table_file):
            return
        
        with open(table_file, "rb") as f:
            data = pickle.load(f)
            self.data = data["data"]
            self.indexes = data["indexes"]

    def real_type(self, args):
        """Mettre le type reel,cad prevenir les erreurs de typage du bd"""
        converted_args = {}
        for field,value in args.items():
            expected_type = self.schema[field]
            value = value.strip() #enlver espace qui peut causer une erreur au type du nombre
            if expected_type == 'int' and (value.isdigit() or value.lstrip('-').isdigit()):
                converted_args[field] = int(value)
            elif expected_type == 'float':#si float,on transforme en float
                try:
                    converted_args[field] = float(value)
                except ValueError:
                    raise ValueError(f"Le champ '{field}' doit être un nombre décimal")
            elif expected_type == 'str':
                if (value.startswith('"') and value.endswith('"')) or \
                    (value.startswith("'") and value.endswith("'")):
                    converted_args[field] = value[1:-1]
                else:
                    converted_args[field] = value
            elif expected_type == 'datetime':
                try:
                    converted_args[field] = datetime.fromisoformat(value)
                except ValueError:
                    raise ValueError(f"Le champ '{field}' doit être au format ISO (YYYY-MM-DDTHH:MM:SS)")
            else:
                converted_args[field] = value

        return converted_args