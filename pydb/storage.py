# storage.py
import os
import json
import pickle
import shutil

class StorageManager:
    def __init__(self, root_dir="./data"):
        """
        Gestionnaire de stockage pour les bases de données.
        
        Args:
            root_dir (str): Répertoire racine pour stocker les données
        """
        self.root_dir = root_dir
        if not os.path.exists(root_dir):
            os.makedirs(root_dir)
    
    def list_databases(self):
        """Liste toutes les bases de données existantes."""
        return [name for name in os.listdir(self.root_dir) 
                if os.path.isdir(os.path.join(self.root_dir, name))]
    
    def database_exists(self, db_name):
        """Vérifie si une base de données existe."""
        return os.path.exists(os.path.join(self.root_dir, db_name))
    
    def create_database(self, db_name):
        """Crée un répertoire pour une nouvelle base de données."""
        db_path = os.path.join(self.root_dir, db_name)
        if os.path.exists(db_path):
            raise ValueError(f"La base de données '{db_name}' existe déjà")
        os.makedirs(db_path)
        return db_path
    
    def delete_database(self, db_name):
        """Supprime une base de données."""
        db_path = os.path.join(self.root_dir, db_name)
        if not os.path.exists(db_path):
            raise ValueError(f"La base de données '{db_name}' n'existe pas")
        shutil.rmtree(db_path)
        return True
    
    def backup_database(self, db_name, backup_dir="./backups"):
        """Crée une sauvegarde d'une base de données."""
        db_path = os.path.join(self.root_dir, db_name)
        if not os.path.exists(db_path):
            raise ValueError(f"La base de données '{db_name}' n'existe pas")
        
        # Créer le répertoire de sauvegarde s'il n'existe pas
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        # Nom du fichier de sauvegarde
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(backup_dir, f"{db_name}_{timestamp}.zip")
        
        # Créer l'archive ZIP
        shutil.make_archive(
            os.path.splitext(backup_file)[0],  # Nom du fichier sans extension
            'zip',
            self.root_dir,
            db_name
        )
        
        return backup_file
    
    def restore_database(self, backup_file, db_name=None):
        """Restaure une base de données à partir d'une sauvegarde."""
        if not os.path.exists(backup_file):
            raise ValueError(f"Le fichier de sauvegarde '{backup_file}' n'existe pas")
        
        # Déterminer le nom de la base de données si non spécifié
        if not db_name:
            import os.path
            basename = os.path.basename(backup_file)
            db_name = basename.split('_')[0]
        
        # Supprimer la base de données si elle existe
        db_path = os.path.join(self.root_dir, db_name)
        if os.path.exists(db_path):
            shutil.rmtree(db_path)
        
        # Extraire l'archive
        import tempfile
        with tempfile.TemporaryDirectory() as temp_dir:
            shutil.unpack_archive(backup_file, temp_dir, 'zip')
            
            # Copier les fichiers extraits
            src_path = os.path.join(temp_dir, db_name)
            if os.path.exists(src_path):
                shutil.copytree(src_path, db_path)
            else:
                raise ValueError(f"Base de données '{db_name}' non trouvée dans la sauvegarde")
        
        return True