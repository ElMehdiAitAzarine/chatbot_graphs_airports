import csv
import pandas as pd
from neo4j import GraphDatabase

# Configuration de Neo4j
NEO4J_URI = "neo4j+s://aebfef88.databases.neo4j.io"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "U5fvbeT04dxS6nXN7vsMQjIvhcBtFogML0a6df5IxnU"

# Définir le chemin du fichier CSV
csv_file_path = "airports.csv"
cleaned_csv_path = "airports_clean.csv"  # Fichier nettoyé avec des NaN

# Connexion à Neo4j
def connect_to_neo4j(uri, username, password):
    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        # Tester la connexion
        with driver.session() as session:
            session.run("RETURN 1")
        print("✅ Connexion à Neo4j réussie.")
        return driver
    except Exception as e:
        print(f"❌ Erreur de connexion à Neo4j : {e}")
        return None

# Nettoyer le fichier CSV : remplacer les champs vides par NaN et sauvegarder le fichier nettoyé
def clean_csv_file(input_path, output_path):
    try:
        # Lire le CSV avec pandas
        df = pd.read_csv(input_path)
        
        # Remplacer les valeurs vides ou '\N' par NaN
        df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)
        df.replace(r'\\N', pd.NA, inplace=True)
        
        # Sauvegarder le CSV nettoyé
        df.to_csv(output_path, index=False)
        print(f"✅ Fichier nettoyé enregistré sous '{output_path}'")
        
    except Exception as e:
        print(f"❌ Erreur lors du nettoyage du fichier CSV : {e}")

# Importer des données depuis un CSV nettoyé
def import_airport_data(driver, csv_file):
    with driver.session() as session:
        try:
            with open(csv_file, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                print("📄 Lecture du fichier CSV réussie. Traitement des données...")

                for row in csv_reader:
                    print(f"🔍 Lecture de la ligne : {row}")
                    airport_name = row.get("Name", "").strip()
                    country = row.get("Country", "").strip()
                    airport_type = row.get("Type", "").strip()

                    # Remplacer les champs vides par 'NaN'
                    airport_name = airport_name if airport_name else 'NaN'
                    country = country if country else 'NaN'
                    airport_type = airport_type if airport_type else 'NaN'

                    print(f"Importation de l'aéroport : {airport_name}")
                    session.write_transaction(create_airport_graph, airport_name, country, airport_type)
                    print(f"✅ Ajout réussi : Aéroport='{airport_name}', Pays='{country}', Type='{airport_type}'")

        except Exception as e:
            print(f"❌ Erreur lors de la lecture ou du traitement du fichier CSV : {e}")

# Créer des nœuds et relations dans Neo4j
def create_airport_graph(tx, airport_name, country, airport_type):
    try:
        query = """
        MERGE (a:Airport {name: $airport_name})
        MERGE (c:Country {code: $country})
        MERGE (t:Type {name: $airport_type})
        MERGE (a)-[:LOCATED_IN]->(c)
        MERGE (a)-[:CLASSIFIED_AS]->(t)
        """
        tx.run(query, airport_name=airport_name, country=country, airport_type=airport_type)
    except Exception as e:
        print(f"❌ Erreur lors de la création des données pour Aéroport='{airport_name}': {e}")

# Point d'entrée principal
if __name__ == "__main__":
    # Nettoyer le fichier CSV avant l'importation
    clean_csv_file(csv_file_path, cleaned_csv_path)
    
    # Se connecter à Neo4j
    driver = connect_to_neo4j(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)
    if driver:
        print("🚀 Début de l'importation des données...")
        import_airport_data(driver, cleaned_csv_path)
        print("✅ Importation terminée.")
        driver.close()
    else:
        print("❌ Impossible de se connecter à Neo4j. Vérifiez vos identifiants et l'URL.")
