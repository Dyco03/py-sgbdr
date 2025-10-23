# relation.py

class Relation:
    def __init__(self, source_table, target_table, relation_type, source_field, target_field):
        """
        Crée une relation entre deux tables.
        
        Args:
            source_table (Table): Table source
            target_table (Table): Table cible
            relation_type (str): Type de relation ('one_to_one', 'one_to_many', 'many_to_many')
            source_field (str): Champ de la table source
            target_field (str): Champ de la table cible
        """
        self.source_table = source_table
        self.target_table = target_table
        self.relation_type = relation_type
        self.source_field = source_field
        self.target_field = target_field
        
        # Vérifier si les champs existent dans les tables
        if source_field not in source_table.schema:
            raise ValueError(f"Le champ '{source_field}' n'existe pas dans la table '{source_table.name}'")
        
        if target_field not in target_table.schema:
            raise ValueError(f"Le champ '{target_field}' n'existe pas dans la table '{target_table.name}'")
        
        # Pour les relations many_to_many, il faut créer une table de jointure
        if relation_type == 'many_to_many':
            self._create_junction_table()
    
    def _create_junction_table(self):
        """Crée une table de jointure pour les relations many_to_many."""
        database = self.source_table.database
        junction_name = f"{self.source_table.name}_{self.target_table.name}_junction"
        
        # Éviter de recréer la table si elle existe déjà
        if junction_name in database.list_tables():
            self.junction_table = database.get_table(junction_name)
            return
        
        # Définir le schéma de la table de jointure
        schema = {
            f"{self.source_table.name}_id": self.source_table.schema[self.source_field],
            f"{self.target_table.name}_id": self.target_table.schema[self.target_field]
        }
        
        # Créer la table de jointure
        self.junction_table = database.create_table(junction_name, schema)
    
    def link(self, source_record, target_record):
        """
        Crée un lien entre deux enregistrements.
        
        Args:
            source_record (dict): Enregistrement de la table source
            target_record (dict): Enregistrement de la table cible
        
        Returns:
            bool: True si la liaison a réussi
        """
        if self.relation_type == 'one_to_one' or self.relation_type == 'one_to_many':
            # Mettre à jour la clé étrangère dans la table cible
            source_id = source_record[self.source_field]
            self.target_table.update(target_record[self.target_table.primary_key], 
                                     {self.target_field: source_id})
            
        elif self.relation_type == 'many_to_many':
            # Ajouter une entrée dans la table de jointure
            source_id = source_record[self.source_field]
            target_id = target_record[self.target_field]
            
            # Vérifier si le lien existe déjà
            existing = self.junction_table.select({
                f"{self.source_table.name}_id": source_id,
                f"{self.target_table.name}_id": target_id
            })
            
            if not existing:
                self.junction_table.insert({
                    f"{self.source_table.name}_id": source_id,
                    f"{self.target_table.name}_id": target_id
                })
        
        return True
    
    def unlink(self, source_record, target_record=None):
        """
        Supprime un lien entre deux enregistrements.
        
        Args:
            source_record (dict): Enregistrement de la table source
            target_record (dict, optional): Enregistrement de la table cible
        
        Returns:
            bool: True si la suppression a réussi
        """
        source_id = source_record[self.source_field]
        
        if self.relation_type == 'one_to_one' or self.relation_type == 'one_to_many':
            if target_record:
                # Mettre à NULL la clé étrangère
                self.target_table.update(target_record[self.target_table.primary_key], 
                                         {self.target_field: None})
            else:
                # Mettre à NULL toutes les références à cette source
                for record in self.target_table.select({self.target_field: source_id}):
                    self.target_table.update(record[self.target_table.primary_key], 
                                             {self.target_field: None})
        
        elif self.relation_type == 'many_to_many':
            if target_record:
                target_id = target_record[self.target_field]
                # Supprimer l'entrée spécifique dans la table de jointure
                junctions = self.junction_table.select({
                    f"{self.source_table.name}_id": source_id,
                    f"{self.target_table.name}_id": target_id
                })
                
                for junction in junctions:
                    self.junction_table.delete(junction[self.junction_table.primary_key])
            else:
                # Supprimer toutes les entrées liées à cette source
                junctions = self.junction_table.select({
                    f"{self.source_table.name}_id": source_id
                })
                
                for junction in junctions:
                    self.junction_table.delete(junction[self.junction_table.primary_key])
        
        return True
    
    def get_related(self, source_record):
        """
        Récupère les enregistrements liés à un enregistrement source.
        
        Args:
            source_record (dict): Enregistrement de la table source
        
        Returns:
            list: Liste des enregistrements liés
        """
        source_id = source_record[self.source_field]
        
        if self.relation_type == 'one_to_one' or self.relation_type == 'one_to_many':
            # Rechercher les enregistrements qui ont la clé étrangère correspondante
            return self.target_table.select({self.target_field: source_id})
        
        elif self.relation_type == 'many_to_many':
            # Récupérer les IDs des cibles dans la table de jointure
            junctions = self.junction_table.select({
                f"{self.source_table.name}_id": source_id
            })
            
            target_ids = [j[f"{self.target_table.name}_id"] for j in junctions]
            
            # Récupérer les enregistrements correspondants
            results = []
            for target_id in target_ids:
                records = self.target_table.select({self.target_field: target_id})
                results.extend(records)
            
            return results