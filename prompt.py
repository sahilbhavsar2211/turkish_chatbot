def sql_generation_prompt(question: str) -> str:
    """
    Generates a SQL query based on the provided question.
    
    Args:
        question (str): The question for which to generate the SQL query.
        
    Returns:
        str: The generated SQL query.
    """
    return f"""
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
    