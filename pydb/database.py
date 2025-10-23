# database.py
import os
import json
import pickle
from .table import Table

class Database:
    def __init__(self, name, storage_dir="./data"):
        self.name = name
        self.tables = {}
        self.storage_dir = os.path.join(storage_dir, name)
        
        # Créer le répertoire de stockage s'il n'existe pas
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
        
        # Charger la base de données si elle existe
        self._load()
    
    def create_table(self, table_name, schema):
        """
        Crée une nouvelle table dans la base de données.
        
        Args:
            table_name (str): Nom de la table
            schema (dict): Schéma de la table {nom_colonne: type_donnée}
        
        Returns:
            Table: L'objet table créé
        """
        if table_name in self.tables:
            raise ValueError(f"La table '{table_name}' existe déjà")
        
        table = Table(table_name, schema, self)
        self.tables[table_name] = table
        self._save()
        return table
    
    def drop_table(self, table_name):
        """Supprime une table de la base de données."""
        if table_name not in self.tables:
            raise ValueError(f"La table '{table_name}' n'existe pas")
        
        # Supprimer le fichier de la table
        table_file = os.path.join(self.storage_dir, f"{table_name}.data")
        if os.path.exists(table_file):
            os.remove(table_file)
        
        del self.tables[table_name]
        self._save()
        return True
    
    def get_table(self, table_name):
        """Récupère une table par son nom."""
        if table_name not in self.tables:
            raise ValueError(f"La table '{table_name}' n'existe pas")
        return self.tables[table_name]
    
    def list_tables(self):
        """Renvoie la liste des tables de la base de données."""
        return list(self.tables.keys())
    
    def _save(self):
        """Sauvegarde les métadonnées de la base de données."""
        metadata = {
            "name": self.name,
            "tables": {name: table.get_metadata() for name, table in self.tables.items()}
        }
        
        metadata_file = os.path.join(self.storage_dir, "metadata.json")
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)
    
    def _load(self):
        """Charge les métadonnées de la base de données."""
        metadata_file = os.path.join(self.storage_dir, "metadata.json")
        if not os.path.exists(metadata_file):
            return
        
        with open(metadata_file, "r") as f:
            metadata = json.load(f)
        
        for table_name, table_metadata in metadata["tables"].items():
            self.tables[table_name] = Table.from_metadata(table_metadata, self)