import psycopg2 as pg

def get_db_connection():
    return pg.connect(
        dbname="your_db_name",
        user="your_db_user",
        password="your_password",
        host="db_host",
        port="5432"
    )

def get_team_name_to_id(db_conn) -> dict:
    """
    Queries the teams table and returns a mapping of normalized team_name → team_id.
    Applies Understat → DB name normalization.

    :param db_conn: psycopg2 connection object
    :return: dict {normalized_team_name: team_id}
    """
    # Name mapping from Understat to your DB
    understat_to_db_name = {
        "Arsenal": "Arsenal",
        "Aston Villa": "Aston Villa",
        "Bournemouth": "AFC Bournemouth",
        "Brentford": "Brentford",
        "Brighton": "Brighton & Hove Albion",
        "Chelsea": "Chelsea",
        "Crystal Palace": "Crystal Palace",
        "Everton": "Everton",
        "Fulham": "Fulham",
        "Ipswich": "Ipswich Town",
        "Leicester": "Leicester City",
        "Liverpool": "Liverpool",
        "Manchester City": "Manchester City",
        "Manchester United": "Manchester United",
        "Newcastle United": "Newcastle United",
        "Nottingham Forest": "Nottingham Forest",
        "Southampton": "Southampton",
        "Tottenham": "Tottenham Hotspur",
        "West Ham": "West Ham United",
        "Wolverhampton Wanderers": "Wolverhampton Wanderers"
    }

    with db_conn.cursor() as cur:
        try:
            cur.execute("SELECT id, name FROM prem_injury.teams")
            rows = cur.fetchall()
            # Base mapping: full DB name → ID
            name_to_id = {name: team_id for team_id, name in rows}

            # Add aliases: short Understat name → same ID
            for short_name, full_name in understat_to_db_name.items():
                if full_name in name_to_id:
                    name_to_id[short_name] = name_to_id[full_name]

            return name_to_id
        except:
            pass