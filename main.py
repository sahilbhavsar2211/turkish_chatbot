from urllib.parse import quote_plus
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import os
try:
    from dotenv import load_dotenv
except ImportError as e:
    print(f"❌ Failed to import python-dotenv: {e}")
    raise
from langchain_openai import ChatOpenAI
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

# Initialize FastAPI app
app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3002",
        "http://localhost",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load environment variables
try:
    load_dotenv()
    print("✅ .env file loaded successfully")
except Exception as e:
    print(f"❌ Failed to load .env file: {e}")
    raise

# Environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Validate environment variables
missing_vars = [var for var, value in [
    ("OPENAI_API_KEY", OPENAI_API_KEY),
    ("DB_USER", DB_USER),
    ("DB_PASSWORD", DB_PASSWORD),
    ("DB_NAME", DB_NAME)
] if not value]
if missing_vars:
    error_msg = f"❌ Missing required environment variables: {', '.join(missing_vars)}"
    print(error_msg)
    raise ValueError(error_msg)

print(f"✅ Environment variables loaded: DB_HOST={DB_HOST}, DB_PORT={DB_PORT}, DB_NAME={DB_NAME}")

# Initialize LLM
try:
    llm = ChatOpenAI(model="gpt-4o", temperature=0, api_key=OPENAI_API_KEY)
    print("✅ LLM initialized successfully")
except Exception as e:
    print(f"❌ Failed to initialize LLM: {e}")
    raise

# Database engine
engine = None

def create_db_engine():
    """Create and test database connection"""
    try:
        encoded_user = quote_plus(DB_USER)
        encoded_password = quote_plus(DB_PASSWORD)
        connection_string = f"mysql+pymysql://{encoded_user}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        print(f"🔍 Attempting to connect to database with: {connection_string.replace(encoded_password, '****')}")
        engine = create_engine(connection_string, echo=False)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"✅ Database connection successful to {DB_NAME}!")
        return engine
    except SQLAlchemyError as e:
        print(f"❌ Database connection failed: {e}")
        print("⚠️ Ensure MySQL server is running and credentials are correct. Try: mysql -u <user> -p -h <host> -P <port> <database>")
        return None
    except Exception as e:
        print(f"❌ Unexpected error during database connection: {e}")
        return None

def list_all_tables(engine):
    """List all tables in the database"""
    try:
        insp = inspect(engine)
        tables = insp.get_table_names()
        print(f"📋 Available tables: {tables}")
        return tables
    except Exception as e:
        print(f"❌ Error listing tables: {e}")
        return []

def get_schema_description(engine, table_names):
    """Get schema descriptions for multiple tables"""
    schema_description = ""
    for table_name in table_names:
        try:
            insp = inspect(engine)
            columns = insp.get_columns(table_name)
            pk_constraint = insp.get_pk_constraint(table_name)
            primary_keys = pk_constraint.get('constrained_columns', []) if pk_constraint else []
            foreign_keys = insp.get_foreign_keys(table_name)
            
            col_list = []
            for col in columns:
                col_info = f'"{col["name"]}" {col["type"]}'
                if col["name"] in primary_keys:
                    col_info += " (PRIMARY KEY)"
                if not col.get("nullable", True):
                    col_info += " NOT NULL"
                col_list.append(col_info)
            
            schema_description += f'Table "{table_name}":\n'
            schema_description += "Columns:\n" + "\n".join([f"  - {col}" for col in col_list])
            
            if foreign_keys:
                schema_description += "\nForeign Keys:\n"
                for fk in foreign_keys:
                    schema_description += f"  - {fk['constrained_columns']} -> {fk['referred_table']}.{fk['referred_columns']}\n"
            
            schema_description += "\n" + "="*50 + "\n"
            
        except Exception as e:
            schema_description += f"❌ Error reading table '{table_name}': {e}\n\n"
    
    print(f"📜 Schema description:\n{schema_description}")
    return schema_description.strip()

def generate_sql_query(user_question):
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
   - Female apps: 'Faladdin: Tarot & Horoscopes', 'Happy Kids • Bebek Gelişimi', 'Happy Mom • Hamilelik Takibi', 'Pregੈ
nancy tracker, 'Hamilelik Takibi'

3. INCOME BRACKET MAPPING (hz_neighborhood_rank):
   - Top 10% / İlk %10 → '10'
   - Top 20% / İlk %20 → '10', '9'  
   - Top 60% / İlk %60 → '10', '9', '8', '7', '6', '5'
   - Top 70% / İlk %70 → '10', '9', '8', '7', '6', '5', '4'
   - Top 90% / İlk %90 → '10', '9', '8', '7', '6', '5', '4', '3', '2'
   - 2nd decile → '2'
   - 3rd decile → '3'
   - 5th decile → '5'
   - 6th decile → '5'
   - 8th decile → '3'
   - 9th decile → '2'
   - High income (yüksek segment) → '10', '9', '8'
   - Middle income (orta segment) → '7', '6', '5'
   - Low income (düşük segment/düşük gelirli) → '4', '3', '2', '1'

4. TOURIST/FOREIGNER IDENTIFICATION:
   - Tourists/foreigners → language NOT IN ('Turkish') for Turkey, language NOT IN ('Dutch') for Netherlands

5. POI CATEGORY MAPPING:
   - Agricultural areas → 'farmland'
   - Textile/Fashion stores → 'clothes'
   - Restaurants → 'restaurant'
   - Treatment centers → 'hospital'
   - Must-see places → 'attraction'
   - Sports areas → 'pitch'
   - Cemetery areas → 'graveyard'
   - Personal care salons → 'hairdresher'
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

7. LANGUAGE MAPPING:
   - Arabic → 'Arabic'
   - Belgian → 'Dutch'
   - Danish → 'Danish'
   - Slovak → 'Slovak'

8. TOURIST/FOREIGNER IDENTIFICATION:
   - For Turkey: language NOT IN ('Turkish')
   - For Netherlands: language NOT IN ('Dutch')

USER QUESTION: {user_question}

INSTRUCTIONS:
1. Always use COUNT(DISTINCT maid) for population counting
2. Use backticks around table names if needed: `nld_table`, `tur_table`
3. Use backticks around column names if they are MySQL reserved words
4. For income brackets, use IN clause with appropriate rank values
5. For multiple cities, use IN ('city1', 'city2', ...) or NOT IN for exclusions
6. For multiple POI categories, use IN ('category1', 'category2', ...)
7. For age ranges, use: age BETWEEN min_age AND max_age
8. For gender identification, use OR condition combining direct gender and app-based inference
9. For tourists/foreigners, use language NOT IN with the local language
10. Handle multiple profiles with IN clause
11. Use proper WHERE clause combinations with AND/OR as needed
12. Use standard MySQL syntax - no double quotes around table/column names
13. Return ONLY the SQL query ready for execution without formatting, backticks, or explanations

SQL QUERY:
"""
    try:
        response = llm.invoke(prompt)
        sql_query = response.content.strip()
        if sql_query.startswith("```sql"):
            sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        elif sql_query.startswith("```"):
            sql_query = sql_query.replace("```", "").strip()
        print(f"✅ Generated SQL Query: {sql_query}")
        return sql_query
    except Exception as e:
        print(f"❌ Error generating SQL: {e}")
        return None

def execute_sql_query(engine, sql_query):
    """Execute SQL query and return results"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql_query))
            if sql_query.strip().upper().startswith('SELECT'):
                rows = result.fetchall()
                columns = result.keys()
                df = pd.DataFrame(rows, columns=columns)
                return df.to_dict(orient="records")
            else:
                conn.commit()
                return {"message": f"Query executed successfully. Rows affected: {result.rowcount}"}
    except SQLAlchemyError as e:
        print(f"❌ SQL execution error: {e}")
        return {"error": f"SQL execution error: {str(e)}"}
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return {"error": f"Unexpected error: {str(e)}"}

def generate_natural_response(user_question, sql_query, query_results):
    """Generate natural language response from query results"""
    prompt = f"""
You are a helpful assistant that explains database query results in natural language.

USER QUESTION: {user_question}

SQL QUERY EXECUTED: {sql_query}

QUERY RESULTS: {query_results}

Please provide a clear, natural language answer to the user's question based on the query results.
If there are no results, explain that no data was found matching the criteria.
"""
    try:
        response = llm.invoke(prompt)
        print(f"✅ Generated natural response: {response.content}")
        return response.content
    except Exception as e:
        print(f"❌ Error generating response: {e}")
        return f"Error generating response: {str(e)}"

# Lifespan handler for startup and shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine
    engine = create_db_engine()
    if not engine:
        error_msg = "Database connection not initialized. Check MySQL server and credentials."
        print(f"❌ {error_msg}")
        raise ValueError(error_msg)
    yield
    if engine:
        engine.dispose()
        print("✅ Database connection closed")

# Attach lifespan handler to FastAPI app
app.lifespan = lifespan

# Pydantic model for request body
class ChatRequest(BaseModel):
    message: str

# API endpoint for chat
@app.post("/api/chat")
async def chat(request: ChatRequest):
    if not engine:
        error_msg = "Database connection not initialized. Check server logs for details."
        print(f"❌ {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)

    user_question = request.message
    print(f"\n🔍 User Question: {user_question}")

    # Generate SQL query
    sql_query = generate_sql_query(user_question)
    if not sql_query:
        error_msg = "Failed to generate SQL query"
        print(f"❌ {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)

    # Execute SQL query
    query_results = execute_sql_query(engine, sql_query)
    if isinstance(query_results, dict) and "error" in query_results:
        print(f"❌ Query execution failed: {query_results['error']}")
        raise HTTPException(status_code=500, detail=query_results["error"])

    # Generate natural language response
    natural_response = generate_natural_response(user_question, sql_query, query_results)

    return {
        "question": user_question,
        "sql_query": sql_query,
        "results": query_results,
        "response": natural_response
    }

# Health check endpoint
@app.get("/health")
async def health():
    return {"status": "ok"}




# import os
# from urllib.parse import quote_plus
# from dotenv import load_dotenv
# from langchain_openai import ChatOpenAI
# import pandas as pd
# from sqlalchemy import create_engine, inspect, text
# from sqlalchemy.exc import SQLAlchemyError
# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware

# # Initialize FastAPI app
# app = FastAPI()

# # Configure CORS to allow requests from Next.js frontend
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["http://localhost:3000"],  # Update with your frontend URL
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# load_dotenv()

# # Environment variables
# OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
# DB_HOST = os.getenv("DB_HOST", "localhost")  # Should be just "localhost" not "Android@localhost"
# DB_PORT = os.getenv("DB_PORT", "3306")
# DB_USER = os.getenv("DB_USER")
# DB_PASSWORD = os.getenv("DB_PASSWORD")
# DB_NAME = os.getenv("DB_NAME")

# # Initialize LLM
# llm = ChatOpenAI(model="gpt-4o", temperature=0, api_key=OPENAI_API_KEY)

# def create_db_engine():
#     """Create and test database connection"""
#     try:
#         # URL-encode the password and username (if needed)
#         encoded_user = quote_plus(DB_USER)
#         encoded_password = quote_plus(DB_PASSWORD)

#         # Create SQLAlchemy engine for MySQL
#         engine = create_engine(
#             f"mysql+pymysql://{encoded_user}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
#             echo=False  # Set to True to see SQL queries
#         )

#         # Test connection
#         with engine.connect() as conn:
#             conn.execute(text("SELECT 1"))

#         print("✅ Database connection successful!")
#         return engine

#     except Exception as e:
#         print(f"❌ Database connection failed: {e}")
#         return None

    
#     except Exception as e:
#         print(f"❌ Database connection failed: {e}")
#         print("Please check your .env file settings:")
#         print(f"  DB_HOST: {DB_HOST}")
#         print(f"  DB_PORT: {DB_PORT}")
#         print(f"  DB_USER: {DB_USER}")
#         print(f"  DB_NAME: {DB_NAME}")
#         return None
    
# def list_all_tables(engine):
#     """List all tables in the database"""
#     try:
#         insp = inspect(engine)
#         return insp.get_table_names()
#     except Exception as e:
#         print(f"❌ Error listing tables: {e}")
#         return []

# def get_schema_description(engine, table_names):
#     """Get schema descriptions for multiple tables"""
#     insp = inspect(engine)
#     schema_description = ""

#     for table_name in table_names:
#         try:
#             columns = insp.get_columns(table_name)
            
#             # Get primary keys
#             pk_constraint = insp.get_pk_constraint(table_name)
#             primary_keys = pk_constraint.get('constrained_columns', []) if pk_constraint else []
            
#             # Get foreign keys
#             foreign_keys = insp.get_foreign_keys(table_name)
            
#             col_list = []
#             for col in columns:
#                 col_info = f'"{col["name"]}" {col["type"]}'
#                 if col["name"] in primary_keys:
#                     col_info += " (PRIMARY KEY)"
#                 if not col.get("nullable", True):
#                     col_info += " NOT NULL"
#                 col_list.append(col_info)
            
#             schema_description += f'Table "{table_name}":\n'
#             schema_description += "Columns:\n" + "\n".join([f"  - {col}" for col in col_list])
            
#             if foreign_keys:
#                 schema_description += "\nForeign Keys:\n"
#                 for fk in foreign_keys:
#                     schema_description += f"  - {fk['constrained_columns']} -> {fk['referred_table']}.{fk['referred_columns']}\n"
            
#             schema_description += "\n" + "="*50 + "\n"
            
#         except Exception as e:
#             schema_description += f"❌ Error reading table '{table_name}': {e}\n\n"

#     return schema_description.strip()

# def generate_sql_query(user_question):
#     """Generate SQL query from user question using LLM"""
#    # Updated SQL Generation Prompt
#     prompt = f"""
# You are a SQL expert. Given the database schema below, generate a SQL query to answer the user's question.

# DATABASE SCHEMA:

# TABLE: nld_table (Netherlands data)
# - maid (VARCHAR): Mobile advertising ID - PRIMARY KEY for counting distinct users
# - gender (VARCHAR): 'Male', 'Female', or empty
# - city (VARCHAR): City names like 'Amsterdam', 'Rotterdam', 'The Hague', etc.
# - year (INT): Year (2022, 2023, 2024, 2025)
# - month (INT): Month (1-12)
# - age (INT): User age (0-100+)
# - language (VARCHAR): 'Dutch', 'English', 'Arabic', 'German', etc.
# - hz_neighborhood_rank (VARCHAR): Income decile ranks ('1'-'10', where '10'=highest income, '1'=lowest)
# - poi_fclass (VARCHAR): Point of interest categories like 'water', 'park', 'pitch', 'graveyard', 'hairdresser', 'residential', 'industrial'
# - profile (VARCHAR): User profiles like 'abroad_vacationer', 'bank_visitor'
# - app (VARCHAR): Mobile app identifiers

# TABLE: tur_table (Turkey data)  
# - maid (VARCHAR): Mobile advertising ID - PRIMARY KEY for counting distinct users
# - gender (VARCHAR): 'Male', 'Female', or empty
# - city (VARCHAR): City names like 'Antalya', 'Amasya', 'Edirne', 'Bingöl', 'Erzincan', 'Erzurum', 'Diyarbakır', etc.
# - year (INT): Year (2022, 2023, 2024, 2025)
# - month (INT): Month (1-12)
# - age (INT): User age (0-100+)
# - language (VARCHAR): 'Turkish', 'Arabic', 'Russian', 'German', 'Dutch', 'Danish', 'Slovak', etc.
# - hz_neighborhood_rank (VARCHAR): Income decile ranks ('1'-'10', where '10'=highest income, '1'=lowest)
# - poi_fclass (VARCHAR): Point of interest categories like 'farmland', 'clothes', 'restaurant', 'hospital', 'attraction', 'pitch', 'nature_reserve', 'mall'
# - profile (VARCHAR): User profiles like 'abroad_vacationer', 'highschool_student', 'football_follower', 'vegan', 'fashion_lover', 'sme'
# - app (VARCHAR): Mobile app identifiers

# IMPORTANT MAPPING RULES:

# 1. COUNTRY IDENTIFICATION:
#    - Turkey/Türkiye → Use table tur_table
#    - Netherlands/Holland/Hollanda → Use table nld_table

# 2. GENDER IDENTIFICATION:
#    - Male/Father/Dad (erkek/baba) → gender = 'Male' OR app IN (male_apps_list)
#    - Female/Mother/Mom (kadın/anne) → gender = 'Female' OR app IN (female_apps_list)
#    - Male apps: 'net.peakgames.Yuzbir', 'Mackolik - Live Sports Results', 'Sahadan Canlı Sonuçlar', 'ASPOR- Canlı Yayın, Spor', 'Okey Plus', 'GOAL Live Scores', 'com.sahibinden', 'Bitcoin Solitaire - Get Real Free Bitcoin!', 'Doviz - Altin, Borsa, Kripto', 'com.supercell.clashofclans2'
#    - Female apps: 'Faladdin: Tarot & Horoscopes', 'Happy Kids • Bebek Gelişimi', 'Happy Mom • Hamilelik Takibi', 'Pregnancy tracker week by week', 'Hamilelik Takibi'

# 3. INCOME BRACKET MAPPING (hz_neighborhood_rank):
#    - Top 10% / İlk %10 → '10'
#    - Top 20% / İlk %20 → '10', '9'  
#    - Top 60% / İlk %60 → '10', '9', '8', '7', '6', '5'
#    - Top 70% / İlk %70 → '10', '9', '8', '7', '6', '5', '4'
#    - Top 90% / İlk %90 → '10', '9', '8', '7', '6', '5', '4', '3', '2'
#    - 2nd decile → '2'
#    - 3rd decile → '3'
#    - 5th decile → '5'
#    - 6th decile → '5'
#    - 8th decile → '3'
#    - 9th decile → '2'
#    - High income (yüksek segment) → '10', '9', '8'
#    - Middle income (orta segment) → '7', '6', '5'
#    - Low income (düşük segment/düşük gelirli) → '4', '3', '2', '1'

# 4. TOURIST/FOREIGNER IDENTIFICATION:
#    - Tourists/Foreigners → language NOT IN ('Turkish') for Turkey, language NOT IN ('Dutch') for Netherlands

# 5. POI CATEGORY MAPPING:
#    - Agricultural areas → 'farmland'
#    - Textile/Fashion stores → 'clothes'
#    - Restaurants → 'restaurant'
#    - Treatment centers → 'hospital'
#    - Must-see places → 'attraction'
#    - Sports areas → 'pitch'
#    - Cemetery areas → 'graveyard'
#    - Personal care salons → 'hairdresser'
#    - Residential areas → 'residential'
#    - Natural living areas → 'nature_reserve'
#    - Shopping mall areas → 'mall'
#    - Industrial zones → 'industrial'

# 6. PROFILE MAPPING:
#    - High school students → 'highschool_student'
#    - Football enthusiasts → 'football_follower'
#    - International travelers → 'abroad_vacationer'
#    - Vegans → 'vegan'
#    - Bank visitors → 'bank_visitor'

# 7. LANGUAGE MAPPING:
#    - Arabic → 'Arabic'
#    - Belgian → 'Dutch'
#    - Danish → 'Danish'
#    - Slovak → 'Slovak'

# 8. TOURIST/FOREIGNER IDENTIFICATION:
#    - For Turkey: language NOT IN ('Turkish')
#    - For Netherlands: language NOT IN ('Dutch')

# USER QUESTION: {user_question}

# INSTRUCTIONS:
# 1. Always use COUNT(DISTINCT maid) for population counting
# 2. Use backticks around table names if needed: `nld_table`, `tur_table`
# 3. Use backticks around column names if they are MySQL reserved words
# 4. For income brackets, use IN clause with appropriate rank values
# 5. For multiple cities, use IN ('city1', 'city2', ...) or NOT IN for exclusions
# 6. For multiple POI categories, use IN ('category1', 'category2', ...)
# 7. For age ranges, use: age BETWEEN min_age AND max_age
# 8. For gender identification, use OR condition combining direct gender and app-based inference
# 9. For tourists/foreigners, use language NOT IN with the local language
# 10. Handle multiple profiles with IN clause
# 11. Use proper WHERE clause combinations with AND/OR as needed
# 12. Use standard MySQL syntax - no double quotes around table/column names
# 13. Return ONLY the SQL query without formatting, backticks, or explanations

# SQL QUERY:
# """

#     try:
#         response = llm.invoke(prompt)
#         sql_query = response.content.strip()
        
#         # Clean up the response (remove markdown if present)
#         if sql_query.startswith("```sql"):
#             sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
#         elif sql_query.startswith("```"):
#             sql_query = sql_query.replace("```", "").strip()
            
#         return sql_query
#     except Exception as e:
#         print(f"❌ Error generating SQL: {e}")
#         return None

# def execute_sql_query(engine, sql_query):
#     """Execute SQL query and return results"""
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(sql_query))
            
#             # For SELECT queries, fetch all results
#             if sql_query.strip().upper().startswith('SELECT'):
#                 rows = result.fetchall()
#                 columns = result.keys()
                
#                 # Convert to pandas DataFrame for better display
#                 df = pd.DataFrame(rows, columns=columns)
#                 return df
#             else:
#                 # For INSERT, UPDATE, DELETE queries
#                 conn.commit()
#                 return f"Query executed successfully. Rows affected: {result.rowcount}"
                
#     except SQLAlchemyError as e:
#         return f"❌ SQL execution error: {e}"
#     except Exception as e:
#         return f"❌ Unexpected error: {e}"

# def generate_natural_response(user_question, sql_query, query_results):
#     """Generate natural language response from query results"""
#     prompt = f"""
# You are a helpful assistant that explains database query results in natural language.

# USER QUESTION: {user_question}

# SQL QUERY EXECUTED: {sql_query}

# QUERY RESULTS: {query_results}

# Please provide a clear, natural language answer to the user's question based on the query results.
# If there are no results, explain that no data was found matching the criteria.
# """
    
#     try:
#         response = llm.invoke(prompt)
#         return response.content
#     except Exception as e:
#         return f"❌ Error generating response: {e}"

# def ask_database_question(engine, user_question):
#     """Main function to process user question and return answer"""
#     print(f"\n🔍 User Question: {user_question}")
#     print("="*80)
    
#     # Step 1: Generate SQL query
#     print("📝 Generating SQL query...")
#     sql_query = generate_sql_query(user_question)
    
#     if not sql_query:
#         return "❌ Failed to generate SQL query"
    
#     print(f"🔤 Generated SQL Query:\n{sql_query}")
#     print("="*80)
    
#     # Step 2: Execute SQL query
#     print("⚡ Executing SQL query...")
#     query_results = execute_sql_query(engine, sql_query)
    
#     print(f"📊 Query Results:\n{query_results}")
#     print("="*80)
    
#     # Step 3: Generate natural language response
#     print("🤖 Generating natural language response...")
#     natural_response = generate_natural_response(user_question, sql_query, query_results)
    
#     print(f"💬 Final Answer:\n{natural_response}")
#     print("="*80)
    
#     return {
#         "question": user_question,
#         "sql_query": sql_query,
#         "results": query_results,
#         "answer": natural_response
#     }

# if __name__ == "__main__":
#     # Create database connection
#     engine = create_db_engine()
    
#     if not engine:
#         print("❌ Cannot proceed without database connection")
#         exit(1)
    
#     try:
#         # Get all tables and schema
#         print("📋 Getting database schema...")
#         all_tables = list_all_tables(engine)
#         print(f"Available tables: {all_tables}")
        
#         if not all_tables:
#             print("❌ No tables found in database")
#             exit(1)
        
#         # schema = get_schema_description(engine, all_tables)
#         # print(f"\n📄 Database Schema:\n{schema}")
        
#         # Interactive user input loop
#         print("\n🚀 Database Assistant Ready!")
#         print("💡 You can ask questions about your database in natural language.")
#         print("💡 Type 'exit', 'quit', or 'q' to stop.\n")
        
#         while True:
#             try:
#                 # Get user input
#                 user_question = input("🤔 Ask your question: ").strip()
                
#                 # Check for exit commands
#                 if user_question.lower() in ['exit', 'quit', 'q', '']:
#                     print("👋 Goodbye!")
#                     break
                
#                 # Process the question
#                 result = ask_database_question(engine, user_question)
#                 print("\n" + "="*100 + "\n")
                
#             except EOFError:
#                 print("\n👋 Goodbye!")
#                 break
    
#     except KeyboardInterrupt:
#         print("\n👋 Goodbye!")
#     except Exception as e:
#         print(f"❌ Unexpected error: {e}")
#     finally:
#         # Close the engine connection
#         if engine:
#             engine.dispose()
#             print("🔌 Database connection closed")
