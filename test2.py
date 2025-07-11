from pydantic import BaseModel
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from langchain.schema.runnable import RunnableLambda  # Add this import above
from langchain_core.output_parsers import StrOutputParser
# Import StrOutputParser
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
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
    # allow_origins=["http://localhost:3000"],
    allow_origins=["*"],
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
- profile (JSON): User profiles like 'abroad_vacationer', 'bank_visitor'
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
- profile (JSON): User profiles like 'abroad_vacationer', 'highschool_student', 'football_follower', 'vegan', 'fashion_lover', 'sme'
- app (VARCHAR): Mobile app identifiers

IMPORTANT MAPPING RULES:

1. COUNTRY IDENTIFICATION:
   - Turkey/Türkiye → Use table tur_table
   - Netherlands/Holland/Hollanda → Use table nld_table

2. GENDER IDENTIFICATION:
   - Male/Father/Dad (erkek/baba) → gender = 'Male' OR app IN (male_apps_list)
   - Female/Mother/Mom (kadın/anne) → gender = 'Female' OR app IN (female_apps_list)
   - Male apps: 'net.peakgames.Yuzbir', 'Mackolik - Live Sports Results', 'Mackolik Canlı Sonuçlar','398157427','Sahadan Canlı Sonuçlar', 'ASPOR- Canlı Yayın, Spor','Karakartal Haber & Canlı Skor', 'Sporx - Spor Haber, Canlı Skor', 'ASPOR-Canlı yayınlar, maç özet', 'FOTOMAÇ–Son dakika spor, haber','FOTOMAÇ–Son dakika spor haberl','Batak - Tekli, Eşli, Koz Maça', 'Okey - İnternetsiz', 'Okey Plus', 'Okey Vip', 'Çanak Okey Plus','101 Okey - İnternetsiz','101 YüzBir Okey Plus','101 Okey HD-İnternetsiz YüzBir',
'101 Okey İnternetsiz HD Yüzbir','1176147574','GOAL Live Scores','Goal live scores','GOAL - Football News & Scores', 'Play Football 2024- Real Goal','6470622761','766443283', 'com.sahibinden','986339882','Football Manager GM - NFL game', 'Bitcoin Solitaire - Get Real Free Bitcoin!','Bitcoin Blocks - Get Bitcoin!','Bitcoin Blast - Earn Bitcoin!','Bitcoin Pop - Get Bitcoin!','Bitcoin Food Fight', 'Doviz - Altin, Borsa, Kripto','A PARA - Borsa, Döviz, Hisse','com.halkaarzhaber.hisseler','Fan of Guns: FPS Pixel Shooter','com.supercell.clashofclans2','com.m3android7s.projects.clashofclansbaselayouts','Mafia Sniper — Wars of Clans'
   - Female apps: '1349245161','1141379201','1517065214','938845147','386022579','289560144','1391655378','Faladdin: Tarot & Horoscopes', 'com.faladdin.app','Happy Kids • Bebek Gelişimi', 'kidslearnigstudios.exerciseforkids.fitnesskidsworkout','com.family.locator.find.my.kids','com.bubadu.doctorkids','com.shaimaa.yogaforkids','com.familylocator.life360.locationtracker.findmykids','com.hippo.coloring_book_kids','Happy Mom • Hamilelik Takibi','mommy.care.games','com.gt.richi.rich.mommy.games.family.simulator','Elika Bebek Gelişimi Takibi','com.kksal55.bebektakibi','Gün Gün Bebek Bakımı, Takibi','com.stillnewagain.bebekgelisim','com.neownd.sound.joy.meditation.sleep.whitenoise','com.lionroar.mommynewbornbabydaycare','pregnant.mom','Pregnancy tracker week by week','Hamilelik Takibi','com.kksal55.hamileliktakibi','com.stillnewagain.hamiletakip'

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
8. For gender identification for turkey, use OR condition combining direct gender and app-based inference
9. For gender identification for netherlands, use only gender column
10. For tourists/foreigners, use language NOT IN with the local language
11. Handle multiple profiles with IN clause and keep in mind that profile is a JSON field
12. Use proper WHERE clause combinations with AND/OR as needed
13. Use standard MySQL syntax - no double quotes around table/column names
14. Return ONLY the SQL query ready for execution without formatting, backticks, or explanations


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


def classification(user_question):
    """
    Classify the user questions to determine whether is it greeting or not.
    """

    prompt = f"""
     You are the classification expert. Classify the user question into one of the following categories:
        - greeting
        - data_query
    USER QUESTION: {user_question}
    INSTRUCTIONS:
    1. If the question is a greeting, classify it as "greeting".
    2. If the question is a data query, classify it as "data_query".
    3. If the questions is not matching any of the above categories, classify it as "data_query".
    4. Return only the classification result like "greeting" or "data_query" without any additional text or formatting.
    5. If prompt is about the capabilities or something like "What can you do?", classify it as "greeting".
    6. If the message is casual or conversational with no clear intent to retrieve data (e.g., "okay", "cool", "nice", "bye", "thanks"), classify it as "greeting".
    """

    try:
        response = llm.invoke(prompt)
        return response.content.strip().lower()

    except Exception as e:
        print(f"❌ Classification error: {e}")
        return "unknown"


def greeting_response(user_question):
    """
    Generate a greeting response.
    """

    prompt = f"""
You are a friendly and polite assistant. Respond appropriately to the user's message.

- If the user says "thank you", respond with a friendly acknowledgment like "You're welcome!" or "Glad I could help!"
- If the user says "you're welcome", "okay", or "cool", respond politely or acknowledge with a short, friendly message.
- If the user says "bye", "goodbye", or ends the conversation, respond with a warm farewell like "Take care!" or "Have a great day!"

USER MESSAGE: {user_question}
    """

    response = llm.invoke(prompt)
    print(f"Greeting response: {response.content.strip()}")
    return response


def handle_classification(user_question):
    """
    Handle the classification of user questions to determine whether it is a greeting or a data query.
    """
    classification_result = classification(user_question)
    print(f"Classification result: {classification_result}")

    if classification_result == "greeting":
        print("User greeting detected.")
        return {
            "question": user_question,
            "sql_query": None,
            "results": None,
            "response": greeting_response(user_question).content.strip()
        }
    elif classification_result == "data_query":
        print("User data query detected.")
        # Proceed to generate SQL query
        return ask_database(user_question=user_question)
    else:
        raise HTTPException(
            status_code=400, detail="Unknown classification result.")


def ask_database(user_question):
    """
    Process the user question to generate SQL query and execute it.
    """
    global engine
    if engine is None:
        raise HTTPException(status_code=500, detail="DB not initialized.")

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


@app.post("/api/chat")
async def chat_endpoint(req: ChatRequest):

    user_question = req.message

    return handle_classification(user_question)

# Health Check


@app.get("/health")
async def health():
    return {"status": "ok"}
