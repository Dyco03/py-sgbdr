# main.py
import sys
import os
import argparse
from .cli import PyDBShell

def main():
    parser = argparse.ArgumentParser(description='PyDB - Système de gestion de base de données en Python')
    parser.add_argument('--db', help='Base de données à utiliser')
    parser.add_argument('--exec', help='Exécuter une requête et quitter')
    
    args = parser.parse_args()
    
    shell = PyDBShell()
    
    # Utiliser une base de données spécifique
    if args.db:
        shell.do_use(args.db)
    
    # Exécuter une requête et quitter
    if args.exec:
        shell.default(args.exec)
        return
    
    # Démarrer le shell interactif
    shell.cmdloop()

if __name__ == "__main__":
    main()