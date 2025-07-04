import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from langchain_openai import ChatOpenAI
import pandas as pd

# Load .env
load_dotenv()

# Global engine placeholder
engine = None

# ENV Vars
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# LangChain LLM
llm = ChatOpenAI(model="gpt-4o", temperature=0, api_key=OPENAI_API_KEY)

# CORS and FastAPI setup
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# DB Connection Creator
def create_db_engine():
    try:
        encoded_user = quote_plus(DB_USER)
        encoded_password = quote_plus(DB_PASSWORD)
        connection_url = f"mysql+pymysql://{encoded_user}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

        engine = create_engine(connection_url, echo=False)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        print("✅ DB connected.")
        return engine
    except SQLAlchemyError as e:
        print(f"❌ SQLAlchemyError: {e}")
        return None
    except Exception as e:
        print(f"❌ General DB error: {e}")
        return None

# Lifespan event to init and dispose engine
@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine
    engine = create_db_engine()
    if not engine:
        print("❌ Engine init failed")
        raise RuntimeError("Database connection failed.")
    yield
    engine.dispose()
    print("✅ DB connection closed")

app.router.lifespan_context = lifespan

# API Models
from pydantic import BaseModel

class ChatRequest(BaseModel):
    message: str

# Core logic
def generate_sql_query(user_question):
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
        sql = response.content.strip()
        return sql.replace("```sql", "").replace("```", "").strip()
    except Exception as e:
        print(f"❌ LLM error: {e}")
        return None

def execute_sql_query(sql_query):
    global engine
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql_query))
            if sql_query.strip().lower().startswith("select"):
                rows = result.fetchall()
                columns = result.keys()
                return pd.DataFrame(rows, columns=columns).to_dict(orient="records")
            else:
                conn.commit()
                return {"message": f"Query executed. Rows affected: {result.rowcount}"}
    except Exception as e:
        return {"error": str(e)}

def generate_natural_response(user_question, sql_query, query_results):
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
        return response.content.strip()
    except Exception as e:
        return f"❌ Failed to generate response: {e}"

# API endpoint
@app.post("/api/chat")
async def chat_endpoint(req: ChatRequest):
    global engine
    if engine is None:
        raise HTTPException(status_code=500, detail="DB not initialized.")

    user_question = req.message
    sql_query = generate_sql_query(user_question)

    if not sql_query:
        raise HTTPException(status_code=500, detail="Failed to generate SQL")

    results = execute_sql_query(sql_query)

    if isinstance(results, dict) and results.get("error"):
        raise HTTPException(status_code=500, detail=results["error"])

    response = generate_natural_response(user_question, sql_query, results)
    return {
        "question": user_question,
        "sql_query": sql_query,
        "results": results,
        "response": response
    }

# Health Check
@app.get("/health")
async def health():
    return {"status": "ok"}
