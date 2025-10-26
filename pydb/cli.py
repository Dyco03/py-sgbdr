# cli.py
import cmd
import os
import sys
from tabulate import tabulate
from .database import Database
from .query import QueryParser
from .storage import StorageManager

class PyDBShell(cmd.Cmd):
    intro = "Bienvenue dans PyDB. Tapez help ou ? pour afficher la liste des commandes."
    prompt = "PyDB> "
    
    def __init__(self):
        super().__init__()
        self.storage = StorageManager()
        self.current_db = None
        self.parser = None
    
    def do_create_db(self, arg):
        """Crée une nouvelle base de données: create_db NOM_DB"""
        if not arg:
            print("Erreur: Nom de base de données requis")
            return
        
        try:
            self.storage.create_database(arg)
            print(f"Base de données '{arg}' créée avec succès")
        except Exception as e:
            print(f"Erreur: {str(e)}")
    
    def do_use(self, arg):
        """Utilise une base de données existante: use NOM_DB"""
        if not arg:
            print("Erreur: Nom de base de données requis")
            return
        
        try:
            if not self.storage.database_exists(arg):
                print(f"Erreur: La base de données '{arg}' n'existe pas")
                return
            
            self.current_db = Database(arg)
            self.parser = QueryParser(self.current_db)
            self.prompt = f"PyDB ({arg})> "
            print(f"Base de données '{arg}' sélectionnée")
        except Exception as e:
            print(f"Erreur: {str(e)}")
    
    def do_show_dbs(self, arg):
        """Affiche la liste des bases de données"""
        dbs = self.storage.list_databases()
        if not dbs:
            print("Aucune base de données trouvée")
            return
        
        print("Bases de données disponibles:")
        for db in dbs:
            print(f"- {db}")
    
    def do_show_tables(self, arg):
        """Affiche la liste des tables de la base de données courante"""
        if not self.current_db:
            print("Erreur: Aucune base de données sélectionnée")
            return
        
        tables = self.current_db.list_tables()
        if not tables:
            print("Aucune table trouvée")
            return
        
        print("Tables disponibles:")
        for table in tables:
            print(f"- {table}")
    
    def do_describe(self, arg):
        """Affiche la structure d'une table: describe NOM_TABLE"""
        if not self.current_db:
            print("Erreur: Aucune base de données sélectionnée")
            return
        
        if not arg:
            print("Erreur: Nom de table requis")
            return
        
        try:
            table = self.current_db.get_table(arg)
            print(f"Structure de la table '{arg}':")
            print(f"Clé primaire: {table.primary_key}")
            print("Colonnes:")
            for col, col_type in table.schema.items():
                print(f"- {col}: {col_type}")
        except Exception as e:
            print(f"Erreur: {str(e)}")
    
    def do_backup(self, arg):
        """Crée une sauvegarde de la base de données courante: backup [CHEMIN]"""
        if not self.current_db:
            print("Erreur: Aucune base de données sélectionnée")
            return
        
        try:
            backup_path = self.storage.backup_database(self.current_db.name, arg if arg else "./backups")
            print(f"Sauvegarde créée: {backup_path}")
        except Exception as e:
            print(f"Erreur: {str(e)}")
    
    def do_restore(self, arg):
        """Restaure une base de données à partir d'une sauvegarde: restore CHEMIN [NOM_DB]"""
        args = arg.split()
        if not args:
            print("Erreur: Chemin de sauvegarde requis")
            return
        
        backup_file = args[0]
        db_name = args[1] if len(args) > 1 else None
        
        try:
            self.storage.restore_database(backup_file, db_name)
            print(f"Base de données restaurée avec succès")
        except Exception as e:
            print(f"Erreur: {str(e)}")
    
    def default(self, line):
        """Traite les requêtes SQL-like."""
        if not self.current_db:
            print("Erreur: Aucune base de données sélectionnée")
            return
        
        try:
            result = self.parser.parse_and_execute(line)
            if "error" in result:
                print(f"Erreur: {result['error']}")
            elif "success" in result:
                print(result["success"])
            elif "data" in result:
                # Afficher les résultats sous forme de tableau
                df = result["data"]
                if df.empty:
                    print("Aucun résultat")
                    return
                print('\n')
                print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))
                
                print(f"\n{len(df)} enregistrement(s) trouvé(s)")
        except Exception as e:
            print(f"Erreur: {str(e)}")
    
    def do_exit(self, arg):
        """Quitte l'application"""
        print("Au revoir!")
        return True
    
    def do_quit(self, arg):
        """Quitte l'application"""
        return self.do_exit(arg)
    
    def do_clear(self, arg):
        """Fait comme clear en linux"""
        os.system('clear')

    def emptyline(self):
        """Ne fait rien quand la ligne est vide"""
        pass