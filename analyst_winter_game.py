import _snowflake
import json
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Définition des constantes pour les modèles sémantiques
DATABASE = "CORTEX_ANALYST_DEMO"
SCHEMA = "WINTER_GAME"
STAGE = "RAW_DATA"
FILES = {
    "Winter Game": "winter_game.yaml",
    "Winter Game Enrichi": "winter_game_enrichi.yaml",
}

# Fonction pour charger et afficher une image depuis un stage Snowflake
def load_and_display_image(stage_path: str):
    session = get_active_session()
    try:
        # Utilisation de get_stream pour récupérer l'image depuis Snowflake stage
        with session.file.get_stream(stage_path) as file_stream:
            image_data = file_stream.read()  # Lecture du contenu du fichier image
            st.image(image_data, use_column_width=True)
    except Exception as e:
        st.error(f"Erreur lors du chargement de l'image : {e}")

def send_message(prompt: str, yaml_file: str) -> dict:
    """Appelle l'API REST et renvoie la réponse avec le modèle sémantique."""
    request_body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ],
        "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILES[yaml_file]}",
    }
    resp = _snowflake.send_snow_api_request(
        "POST",
        f"/api/v2/cortex/analyst/message",
        {},
        {},
        request_body,
        {},
        30000,
    )
    if resp["status"] < 400:
        return json.loads(resp["content"])
    else:
        raise Exception(f"Failed request with status {resp['status']}: {resp}")

def process_message(prompt: str, yaml_file: str) -> None:
    """Traite un message et ajoute la réponse au chat."""
    st.session_state.messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("Génération de la réponse..."):
            response = send_message(prompt=prompt, yaml_file=yaml_file)
            content = response["message"]["content"]
            display_content(content=content)
    st.session_state.messages.append({"role": "assistant", "content": content})

def display_content(content: list, message_index: int = None) -> None:
    """Affiche le contenu d'un message."""
    message_index = message_index or len(st.session_state.messages)
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                        st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            with st.expander("Requête SQL", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("Résultats", expanded=True):
                with st.spinner("Exécution de la requête SQL..."):
                    session = get_active_session()
                    df = session.sql(item["statement"]).to_pandas()
                    if len(df.index) > 1:
                        data_tab, line_tab, bar_tab = st.tabs(
                            ["Données", "Graphique en ligne", "Graphique en barres"]
                        )
                        data_tab.dataframe(df)
                        if len(df.columns) > 1:
                            df = df.set_index(df.columns[0])
                        with line_tab:
                            st.line_chart(df)
                        with bar_tab:
                            st.bar_chart(df)
                    else:
                        st.dataframe(df)

# Interface utilisateur Streamlit
st.title("Cortex Analyst - Winter Game")
st.markdown("Modèles Sémantiques pour Winter Game")

# Affichage des onglets pour les différents modèles
tab1, tab2 = st.tabs(["Winter Game", "Winter Game Enrichi"])

with tab1:
    st.header("Modèle Winter Game")
    st.markdown("Fonctionnalités pour le modèle Winter Game.")

with tab2:
    st.header("Modèle Winter Game Enrichi")
    st.markdown("Fonctionnalités pour le modèle Winter Game Enrichi.")

# Charger et afficher l'image depuis le stage Snowflake
stage_path = '@"CORTEX_ANALYST_DEMO"."WINTER_GAME"."RAW_DATA"/winter_olympic_logo.png'
load_and_display_image(stage_path)

# Logique de gestion du chat
if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.suggestions = []
    st.session_state.active_suggestion = None

for message_index, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        display_content(content=message["content"], message_index=message_index)

if user_input := st.chat_input("Quelle est votre question ?"):
    process_message(prompt=user_input, yaml_file="Winter Game")

if st.session_state.active_suggestion:
    process_message(prompt=st.session_state.active_suggestion, yaml_file="Winter Game")
    st.session_state.active_suggestion = None
