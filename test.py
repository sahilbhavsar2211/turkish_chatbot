import os
import streamlit as st
from urllib.parse import quote_plus
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
import time

# Page configuration
st.set_page_config(
    page_title="Database Assistant",
    page_icon="🤖",
    layout="wide"
)

# Load environment variables
load_dotenv()

# Initialize session state
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# Simple CSS for chat interface
st.markdown("""
<style>
    .chat-container {
        max-height: 600px;
        overflow-y: auto;
        padding: 20px;
        border: 1px solid #ddd;
        border-radius: 10px;
        background-color: #f9f9f9;
        margin-bottom: 20px;
    }
    .user-message {
        background-color: #007bff;
        color: white;
        padding: 10px 15px;
        border-radius: 20px;
        margin: 10px 0;
        max-width: 70%;
        margin-left: auto;
        text-align: right;
    }
    .assistant-message {
        background-color: #e9ecef;
        color: #333;
        padding: 10px 15px;
        border-radius: 20px;
        margin: 10px 0;
        max-width: 70%;
        margin-right: auto;
    }
    .sql-expander {
        margin-top: 10px;
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 5px;
    }
    .header {
        text-align: center;
        padding: 20px;
        background: linear-gradient(90deg, #007bff, #0056b3);
        color: white;
        border-radius: 10px;
        margin-bottom: 30px;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def initialize_llm():
    """Initialize and cache the LLM"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        st.error("❌ OpenAI API key not found. Please set OPENAI_API_KEY in your .env file.")
        st.stop()
    return ChatOpenAI(model="gpt-4o", temperature=0, api_key=api_key)

@st.cache_resource
def create_db_engine():
    """Create and cache database connection"""
    try:
        # Get environment variables
        DB_HOST = os.getenv("DB_HOST", "localhost")
        DB_PORT = os.getenv("DB_PORT", "3306")
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_NAME = os.getenv("DB_NAME")
        
        if not all([DB_USER, DB_PASSWORD, DB_NAME]):
            st.error("❌ Database credentials not found. Please check your .env file.")
            st.stop()
        
        # URL-encode credentials
        encoded_user = quote_plus(DB_USER)
        encoded_password = quote_plus(DB_PASSWORD)
        
        # Create SQLAlchemy engine
        engine = create_engine(
            f"mysql+pymysql://{encoded_user}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
            echo=False
        )
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        return engine
        
    except Exception as e:
        st.error(f"❌ Database connection failed: {e}")
        st.stop()

def generate_sql_query(user_question, llm):
    """Generate SQL query from user question using LLM"""
    prompt = f"""
You are a SQL expert. Given the database schema below, generate a SQL query to answer the user's question.

DATABASE SCHEMA:

TABLE: nld_table (Netherlands data)
- maid (VARCHAR): Mobile advertising ID - PRIMARY KEY for counting distinct users
- gender (VARCHAR): 'Male', 'Female', or empty
- city (VARCHAR): City names like 'Amsterdam', 'Rotterdam', 'The Hague', etc.
- year (INT): Year (2022, 2023, 2024, 2025)
- month (INT): Month (1-12)
- age (INT): User age (0-100+)
- language (VARCHAR): 'Dutch', 'English', 'Arabic', 'German', etc.
- hz_neighborhood_rank (VARCHAR): Income decile ranks ('1'-'10', where '10'=highest income, '1'=lowest)
- poi_fclass (VARCHAR): Point of interest categories like 'water', 'park', 'pitch', 'graveyard', 'hairdresser', 'residential', 'industrial'
- profile (VARCHAR): User profiles like 'abroad_vacationer', 'bank_visitor'
- app (VARCHAR): Mobile app identifiers

TABLE: tur_table (Turkey data)  
- maid (VARCHAR): Mobile advertising ID - PRIMARY KEY for counting distinct users
- gender (VARCHAR): 'Male', 'Female', or empty
- city (VARCHAR): City names like 'Antalya', 'Amasya', 'Edirne', 'Bingöl', 'Erzincan', 'Erzurum', 'Diyarbakır', etc.
- year (INT): Year (2022, 2023, 2024, 2025)
- month (INT): Month (1-12)
- age (INT): User age (0-100+)
- language (VARCHAR): 'Turkish', 'Arabic', 'Russian', 'German', 'Dutch', 'Danish', 'Slovak', etc.
- hz_neighborhood_rank (VARCHAR): Income decile ranks ('1'-'10', where '10'=highest income, '1'=lowest)
- poi_fclass (VARCHAR): Point of interest categories like 'farmland', 'clothes', 'restaurant', 'hospital', 'attraction', 'pitch', 'nature_reserve', 'mall'
- profile (VARCHAR): User profiles like 'abroad_vacationer', 'highschool_student', 'football_follower', 'vegan', 'fashion_lover', 'sme'
- app (VARCHAR): Mobile app identifiers

IMPORTANT MAPPING RULES:

1. COUNTRY IDENTIFICATION:
   - Turkey/Türkiye → Use table tur_table
   - Netherlands/Holland/Hollanda → Use table nld_table

2. GENDER IDENTIFICATION:
   - Male/Father/Dad (erkek/baba) → gender = 'Male' OR app IN (male_apps_list)
   - Female/Mother/Mom (kadın/anne) → gender = 'Female' OR app IN (female_apps_list)
   - Male apps: 'net.peakgames.Yuzbir', 'Mackolik - Live Sports Results', 'Sahadan Canlı Sonuçlar', 'ASPOR- Canlı Yayın, Spor', 'Okey Plus', 'GOAL Live Scores', 'com.sahibinden', 'Bitcoin Solitaire - Get Real Free Bitcoin!', 'Doviz - Altin, Borsa, Kripto', 'com.supercell.clashofclans2'
   - Female apps: 'Faladdin: Tarot & Horoscopes', 'Happy Kids • Bebek Gelişimi', 'Happy Mom • Hamilelik Takibi', 'Pregnancy tracker week by week', 'Hamilelik Takibi'

3. INCOME BRACKET MAPPING (hz_neighborhood_rank):
   - Top 10% / İlk %10 → '10'
   - Top 20% / İlk %20 → '10', '9'  
   - Top 60% / İlk %60 → '10', '9', '8', '7', '6', '5'
   - Top 70% / İlk %70 → '10', '9', '8', '7', '6', '5', '4'
   - Top 90% / İlk %90 → '10', '9', '8', '7', '6', '5', '4', '3', '2'
   - High income (yüksek segment) → '10', '9', '8'
   - Middle income (orta segment) → '7', '6', '5'
   - Low income (düşük segment/düşük gelirli) → '4', '3', '2', '1'

4. TOURIST/FOREIGNER IDENTIFICATION:
   - For Turkey: language NOT IN ('Turkish')
   - For Netherlands: language NOT IN ('Dutch')

5. POI CATEGORY MAPPING:
   - Agricultural areas → 'farmland'
   - Textile/Fashion stores → 'clothes'
   - Restaurants → 'restaurant'
   - Treatment centers → 'hospital'
   - Must-see places → 'attraction'
   - Sports areas → 'pitch'
   - Cemetery areas → 'graveyard'
   - Personal care salons → 'hairdresser'
   - Residential areas → 'residential'
   - Natural living areas → 'nature_reserve'
   - Shopping mall areas → 'mall'
   - Industrial zones → 'industrial'

6. PROFILE MAPPING:
   - High school students → 'highschool_student'
   - Football enthusiasts → 'football_follower'
   - International travelers → 'abroad_vacationer'
   - Vegans → 'vegan'
   - Bank visitors → 'bank_visitor'

USER QUESTION: {user_question}

INSTRUCTIONS:
1. Always use COUNT(DISTINCT maid) for population counting
2. Use backticks around table names if needed: `nld_table`, `tur_table`
3. For income brackets, use IN clause with appropriate rank values
4. For multiple cities, use IN ('city1', 'city2', ...) or NOT IN for exclusions
5. For age ranges, use: age BETWEEN min_age AND max_age
6. For gender identification, use OR condition combining direct gender and app-based inference
7. Use standard MySQL syntax
8. Return ONLY the SQL query without formatting, backticks, or explanations

SQL QUERY:
"""
    
    try:
        response = llm.invoke(prompt)
        sql_query = response.content.strip()
        
        # Clean up the response
        if sql_query.startswith("```sql"):
            sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        elif sql_query.startswith("```"):
            sql_query = sql_query.replace("```", "").strip()
            
        return sql_query
    except Exception as e:
        return f"Error generating SQL: {e}"

def execute_sql_query(engine, sql_query):
    """Execute SQL query and return results"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql_query))
            
            if sql_query.strip().upper().startswith('SELECT'):
                rows = result.fetchall()
                columns = result.keys()
                df = pd.DataFrame(rows, columns=columns)
                return df
            else:
                conn.commit()
                return f"Query executed successfully. Rows affected: {result.rowcount}"
                
    except SQLAlchemyError as e:
        return f"SQL execution error: {e}"
    except Exception as e:
        return f"Unexpected error: {e}"

def generate_natural_response(user_question, sql_query, query_results, llm):
    """Generate natural language response from query results"""
    prompt = f"""
You are a helpful assistant that explains database query results in natural language.

USER QUESTION: {user_question}

SQL QUERY EXECUTED: {sql_query}

QUERY RESULTS: {query_results}

Please provide a clear, natural language answer to the user's question based on the query results.
If there are no results, explain that no data was found matching the criteria.
Include specific numbers and insights from the data.
"""
    
    try:
        response = llm.invoke(prompt)
        return response.content
    except Exception as e:
        return f"Error generating response: {e}"

def display_chat_history():
    """Display chat history"""
    if st.session_state.chat_history:
        
        for chat in st.session_state.chat_history:
            # User message
            st.markdown(f'<div class="user-message">{chat["user"]}</div>', unsafe_allow_html=True)
            
            # Assistant message
            st.markdown(f'<div class="assistant-message">{chat["assistant"]}</div>', unsafe_allow_html=True)
            
            # SQL query expander
            if chat.get("sql_query"):
                with st.expander("View Generated SQL Query"):
                    st.code(chat["sql_query"], language='sql')
            
            # Results expander
            if chat.get("results") is not None:
                with st.expander("View Query Results"):
                    if isinstance(chat["results"], pd.DataFrame):
                        st.dataframe(chat["results"])
                    else:
                        st.text(chat["results"])
        
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('</div>', unsafe_allow_html=True)

def main():

    # Initialize components
    llm = initialize_llm()
    engine = create_db_engine()
    
    # Chat History Container
    display_chat_history()
    
    # User input
    user_input = st.text_input("Ask your question:", placeholder="e.g., How many male people from the Netherlands?")
    
    if st.button("Send") and user_input.strip():
        # Generate SQL query
        sql_query = generate_sql_query(user_input, llm)
        
        if sql_query and not sql_query.startswith("Error"):
            # Execute query
            query_results = execute_sql_query(engine, sql_query)
            
            if isinstance(query_results, pd.DataFrame):
                # Generate natural language response
                natural_response = generate_natural_response(
                    user_input, sql_query, query_results, llm
                )
                
                # Add to chat history
                st.session_state.chat_history.append({
                    "user": user_input,
                    "assistant": natural_response,
                    "sql_query": sql_query,
                    "results": query_results
                })
                
            else:
                # Error case
                st.session_state.chat_history.append({
                    "user": user_input,
                    "assistant": f"I encountered an error: {query_results}",
                    "sql_query": sql_query,
                    "results": None
                })
        else:
            # SQL generation error
            st.session_state.chat_history.append({
                "user": user_input,
                "assistant": f"I couldn't generate a proper SQL query. Error: {sql_query}",
                "sql_query": None,
                "results": None
            })
        
        # Rerun to show updated chat
        st.rerun()

if __name__ == "__main__":
    main()