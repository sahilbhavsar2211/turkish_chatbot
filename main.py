import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

# Environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DB_HOST = os.getenv("DB_HOST", "localhost")  # Should be just "localhost" not "Android@localhost"
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Initialize LLM
llm = ChatOpenAI(model="gpt-4o", temperature=0, api_key=OPENAI_API_KEY)

def create_db_engine():
    """Create and test database connection"""
    try:
        # URL-encode the password and username (if needed)
        encoded_user = quote_plus(DB_USER)
        encoded_password = quote_plus(DB_PASSWORD)

        # Create SQLAlchemy engine for MySQL
        engine = create_engine(
            f"mysql+pymysql://{encoded_user}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
            echo=False  # Set to True to see SQL queries
        )

        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        print("✅ Database connection successful!")
        return engine

    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

    
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("Please check your .env file settings:")
        print(f"  DB_HOST: {DB_HOST}")
        print(f"  DB_PORT: {DB_PORT}")
        print(f"  DB_USER: {DB_USER}")
        print(f"  DB_NAME: {DB_NAME}")
        return None
    
def list_all_tables(engine):
    """List all tables in the database"""
    try:
        insp = inspect(engine)
        return insp.get_table_names()
    except Exception as e:
        print(f"❌ Error listing tables: {e}")
        return []

def get_schema_description(engine, table_names):
    """Get schema descriptions for multiple tables"""
    insp = inspect(engine)
    schema_description = ""

    for table_name in table_names:
        try:
            columns = insp.get_columns(table_name)
            
            # Get primary keys
            pk_constraint = insp.get_pk_constraint(table_name)
            primary_keys = pk_constraint.get('constrained_columns', []) if pk_constraint else []
            
            # Get foreign keys
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

    return schema_description.strip()

def generate_sql_query(user_question):
    """Generate SQL query from user question using LLM"""
    prompt = f"""
You are a SQL expert. Given the database schema below, generate a SQL query to answer the user's question.

DATABASE SCHEMA:

TABLE: nld_table
Description: Netherlands location and device data
Columns (38 total):
- __time (TIMESTAMP): timestamp format 2024-01-30T15:00:00.000Z
- gender (VARCHAR): Male/Female or empty
- city (VARCHAR): Amsterdam, Almere, etc. or empty
- year (INT): 2024
- latitude (FLOAT): geographical latitude
- wz_district (VARCHAR): district names like Vinkel, Alkmaar
- language (VARCHAR): Arabic, English, Dutch, etc.
- poi_name (VARCHAR): point of interest names like Vondelpark or empty
- hour (INT): 0-23 hour of day
- ipv4 (VARCHAR): IP addresses like 143.179.120.164
- ipv6 (VARCHAR): IPv6 addresses, mostly empty
- geohash (VARCHAR): geohash strings like u15y5sux
- hz_city (VARCHAR): city names or empty
- wz_city (VARCHAR): city names like Rotterdam, Vinkel
- id_type (INT): 0 or 1
- model (VARCHAR): device models like Galaxy S10, Redmi Note 9
- hz_neighborhood_rank (VARCHAR): mostly empty
- maid (VARCHAR): UUID format mobile advertising ID
- make (VARCHAR): device manufacturers like samsung, xiaomi
- day (INT): day of month (1-31)
- hz_district (VARCHAR): district names like Almere or empty
- longitude (FLOAT): geographical longitude
- timestamp (BIGINT): Unix timestamp
- day_of_week (INT): day of week (1-7)
- app (VARCHAR): app identifiers like com.fugo.wow
- connection_type (VARCHAR): Wifi, 4G, etc.
- profile (VARCHAR): user profiles, mostly empty
- wz_neighborhood_rank (INT): neighborhood ranking like 47, 59
- lin_name (VARCHAR): location names like Damrak or empty
- poi_fclass (VARCHAR): POI categories like water, park
- minute (INT): 0-59 minute of hour
- wz_neighborhood (VARCHAR): neighborhood names like Haarlem, Weesp
- hz_neighborhood (VARCHAR): neighborhood names or empty
- carrier (VARCHAR): telecom carriers like Kpn, Vodafone Nl
- lin_fclass (VARCHAR): location categories like canal or empty
- month (INT): month number (1-12)
- district (VARCHAR): district names like Tilburg, Zevenaar
- neighborhood (VARCHAR): neighborhood names like Spijkenisse, Leiderdorp
- age (INT): user age 0-100+

TABLE: tur_table
Description: Turkey location and device data
Columns (42 total):
- __time (TIMESTAMP): timestamp format 2024-05-26T11:00:00.000Z
- gender (VARCHAR): mostly empty
- city (VARCHAR): Antalya, etc. or empty
- year (INT): 2024
- wz_city_rank (INT): city ranking 0-10
- latitude (FLOAT): geographical latitude
- wz_district (VARCHAR): district names like Alanya, Konyaaltı
- language (VARCHAR): Turkish, Arabic, Russian, German, etc.
- poi_name (VARCHAR): POI names like Gator, Hippie Cat or empty
- hour (INT): 0-23 hour of day
- ipv4 (VARCHAR): IP addresses like 178.240.129.0
- ipv6 (VARCHAR): IPv6 addresses, mostly empty
- geohash (VARCHAR): geohash strings like swqzbhh7
- hz_city (VARCHAR): city names like Antalya, Hatay
- wz_city (VARCHAR): city names like Antalya, Denizli
- id_type (INT): 0 or 1
- model (VARCHAR): device models like a10s, dandelion or empty
- wz_district_rank (INT): district ranking 0-10
- hz_neighborhood_rank (INT): neighborhood ranking 0-10
- maid (VARCHAR): UUID format mobile advertising ID
- make (VARCHAR): device manufacturers like samsung, xiaomi, huawei
- day (INT): day of month (1-31)
- hz_district (VARCHAR): district names like Alanya, Konyaaltı
- longitude (FLOAT): geographical longitude
- timestamp (BIGINT): Unix timestamp
- day_of_week (INT): day of week (1-7)
- app (VARCHAR): app identifiers like com.jaumo, com.viber.voip
- connection_type (VARCHAR): Wifi, 3G, 4G, etc.
- profile (VARCHAR): user profiles like fashion_lover, sme or empty
- wz_neighborhood_rank (INT): neighborhood ranking 0-10
- lin_name (VARCHAR): street names like Milli Egemenlik Caddesi or empty
- poi_fclass (VARCHAR): POI categories like clothes, farmland
- hz_city_rank (INT): city ranking 0-10
- minute (INT): 0-59 minute of hour
- wz_neighborhood (VARCHAR): neighborhood names like Şekerhane Mahallesi or empty
- hz_neighborhood (VARCHAR): neighborhood names like Çarşı Mahallesi or empty
- carrier (VARCHAR): telecom carriers like Turkcell, Vodafone Tr
- lin_fclass (VARCHAR): location categories like tertiary, residential or empty
- month (INT): month number (1-12)
- district (VARCHAR): district names like Alanya, Kumluca
- neighborhood (VARCHAR): neighborhood names like Çarşı Mahallesi or empty
- age (INT): user age 0-100+
- hz_district_rank (INT): district ranking 0-10

USER QUESTION: {user_question}

INSTRUCTIONS:
1. Generate ONLY the SQL query, no explanations
2. Use proper MySQL syntax
3. Make sure the query is syntactically correct and follows MySQL conventions
4. Use appropriate JOINs if multiple tables are needed
5. Include appropriate WHERE clauses, GROUP BY, ORDER BY, and LIMIT as needed
6. Handle potential NULL values appropriately using IS NULL or IS NOT NULL
7. Use proper aggregation functions (COUNT, SUM, AVG, MAX, MIN) when needed
8. For time-based queries, use appropriate date/time functions
9. Use DISTINCT maid to avoid duplicate entries in results
10. Consider performance by adding appropriate conditions to limit result sets
11. Use proper column aliases for readability when needed
12. Return only the SQL query without any markdown formatting, backticks, or extra text

SQL QUERY:
"""
    
    try:
        response = llm.invoke(prompt)
        sql_query = response.content.strip()
        
        # Clean up the response (remove markdown if present)
        if sql_query.startswith("```sql"):
            sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        elif sql_query.startswith("```"):
            sql_query = sql_query.replace("```", "").strip()
            
        return sql_query
    except Exception as e:
        print(f"❌ Error generating SQL: {e}")
        return None

def execute_sql_query(engine, sql_query):
    """Execute SQL query and return results"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql_query))
            
            # For SELECT queries, fetch all results
            if sql_query.strip().upper().startswith('SELECT'):
                rows = result.fetchall()
                columns = result.keys()
                
                # Convert to pandas DataFrame for better display
                df = pd.DataFrame(rows, columns=columns)
                return df
            else:
                # For INSERT, UPDATE, DELETE queries
                conn.commit()
                return f"Query executed successfully. Rows affected: {result.rowcount}"
                
    except SQLAlchemyError as e:
        return f"❌ SQL execution error: {e}"
    except Exception as e:
        return f"❌ Unexpected error: {e}"

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
        return response.content
    except Exception as e:
        return f"❌ Error generating response: {e}"

def ask_database_question(engine, user_question):
    """Main function to process user question and return answer"""
    print(f"\n🔍 User Question: {user_question}")
    print("="*80)
    
    # Step 1: Generate SQL query
    print("📝 Generating SQL query...")
    sql_query = generate_sql_query(user_question)
    
    if not sql_query:
        return "❌ Failed to generate SQL query"
    
    print(f"🔤 Generated SQL Query:\n{sql_query}")
    print("="*80)
    
    # Step 2: Execute SQL query
    print("⚡ Executing SQL query...")
    query_results = execute_sql_query(engine, sql_query)
    
    print(f"📊 Query Results:\n{query_results}")
    print("="*80)
    
    # Step 3: Generate natural language response
    print("🤖 Generating natural language response...")
    natural_response = generate_natural_response(user_question, sql_query, query_results)
    
    print(f"💬 Final Answer:\n{natural_response}")
    print("="*80)
    
    return {
        "question": user_question,
        "sql_query": sql_query,
        "results": query_results,
        "answer": natural_response
    }

if __name__ == "__main__":
    # Create database connection
    engine = create_db_engine()
    
    if not engine:
        print("❌ Cannot proceed without database connection")
        exit(1)
    
    try:
        # Get all tables and schema
        print("📋 Getting database schema...")
        all_tables = list_all_tables(engine)
        print(f"Available tables: {all_tables}")
        
        if not all_tables:
            print("❌ No tables found in database")
            exit(1)
        
        # schema = get_schema_description(engine, all_tables)
        # print(f"\n📄 Database Schema:\n{schema}")
        
        # Interactive user input loop
        print("\n🚀 Database Assistant Ready!")
        print("💡 You can ask questions about your database in natural language.")
        print("💡 Type 'exit', 'quit', or 'q' to stop.\n")
        
        while True:
            try:
                # Get user input
                user_question = input("🤔 Ask your question: ").strip()
                
                # Check for exit commands
                if user_question.lower() in ['exit', 'quit', 'q', '']:
                    print("👋 Goodbye!")
                    break
                
                # Process the question
                result = ask_database_question(engine, user_question)
                print("\n" + "="*100 + "\n")
                
            except EOFError:
                print("\n👋 Goodbye!")
                break
    
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        # Close the engine connection
        if engine:
            engine.dispose()
            print("🔌 Database connection closed")




# import os
# from dotenv import load_dotenv
# from langchain_openai import ChatOpenAI
# import pandas as pd
# from sqlalchemy import create_engine, inspect, text
# from sqlalchemy.exc import SQLAlchemyError

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


# from sqlalchemy import create_engine, text
# from urllib.parse import quote_plus

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

# def generate_sql_query(user_question, schema):
#     """Generate SQL query from user question using LLM"""
#     prompt = f"""
# You are a SQL expert. Given the database schema below, generate a SQL query to answer the user's question.

# DATABASE SCHEMA:
# {schema}

# USER QUESTION: {user_question}

# INSTRUCTIONS:
# 1. Generate ONLY the SQL query, no explanations
# 2. Use proper MySQL syntax
# 3. Make sure the query is syntactically correct
# 4. Use appropriate JOINs if multiple tables are needed
# 5. Include appropriate WHERE clauses, GROUP BY, ORDER BY as needed
# 6. Return only the SQL query without any markdown formatting or extra text

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

# def ask_database_question(engine, user_question, schema):
#     """Main function to process user question and return answer"""
#     print(f"\n🔍 User Question: {user_question}")
#     print("="*80)
    
#     # Step 1: Generate SQL query
#     print("📝 Generating SQL query...")
#     sql_query = generate_sql_query(user_question, schema)
    
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
        
#         schema = get_schema_description(engine, all_tables)
#         print(f"\n📄 Database Schema:\n{schema}")
        
#         # Example questions - you can modify these
#         questions = [
#             "What is the population in Türkiye, living in Amasya or Antalya, using agricultural production areas or textile store areas?",
#             "Show me all records from the re_agentic_chatbot table",
#             "What are the different types of areas available in the database?"
#         ]
        
#         # Process each question
#         for question in questions:
#             result = ask_database_question(engine, question, schema)
#             print("\n" + "="*100 + "\n")
    
#     except KeyboardInterrupt:
#         print("\n👋 Goodbye!")
#     except Exception as e:
#         print(f"❌ Unexpected error: {e}")
#     finally:
#         # Close the engine connection
#         if engine:
#             engine.dispose()
#             print("🔌 Database connection closed")