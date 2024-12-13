from neo4j import GraphDatabase
import streamlit as st

# Configuration de Neo4j
NEO4J_URI = "neo4j+s://aebfef88.databases.neo4j.io"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "U5fvbeT04dxS6nXN7vsMQjIvhcBtFogML0a6df5IxnU"

# Connexion à Neo4j
def connect_to_neo4j(uri, username, password):
    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        return driver
    except Exception as e:
        print(f"Erreur de connexion à Neo4j : {e}")
        return None

# Générer une requête Cypher basée sur une question
def generate_cypher_query(question):
    question = question.strip().replace("?", "")
    if "type" in question.lower():
        airport_type = question.split("type")[-1].strip()
        return f"MATCH (a:Airport)-[:CLASSIFIED_AS]->(t:Type) WHERE toLower(t.name) CONTAINS toLower('{airport_type}') RETURN a.name AS Airport"
    elif "pays" in question.lower() or "localisé" in question.lower():
        country = question.split("pays")[-1].strip()
        return f"MATCH (a:Airport)-[:LOCATED_IN]->(c:Country) WHERE toLower(c.code) CONTAINS toLower('{country}') RETURN a.name AS Airport"
    else:
        return None

# Exécuter une requête Neo4j
def query_neo4j(driver, query):
    with driver.session() as session:
        result = session.run(query)
        return [record.data() for record in result]

# Interface utilisateur avec Streamlit
def chatbot_interface():
    st.title("Chatbot sur les Aéroports avec Neo4j")
    st.write("Posez une question sur les aéroports dans votre base de données Neo4j!")

    # Connexion à Neo4j
    driver = connect_to_neo4j(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)

    # Saisie utilisateur
    question = st.text_input("Entrez votre question (ex. 'Quels aéroports sont localisés dans le pays US ?') :")
    if st.button("Poser la question"):
        if question:
            # Générer une requête Cypher basée sur la question
            query = generate_cypher_query(question)
            if query:
                st.write(f"Requête générée : `{query}`")
                # Exécuter la requête et afficher les résultats
                data = query_neo4j(driver, query)
                if data:
                    st.write("Résultats trouvés :")
                    for record in data:
                        st.write(f"- {record['Airport']}")
                else:
                    st.write("Aucun résultat trouvé.")
            else:
                st.write("Je ne comprends pas cette question. Essayez de reformuler.")
        else:
            st.warning("Veuillez entrer une question.")

    # Fermer la connexion Neo4j
    if driver:
        driver.close()

if __name__ == "__main__":
    chatbot_interface()
