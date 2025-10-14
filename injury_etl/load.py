
def create_injury_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
    CREATE SCHEMA IF NOT EXISTS prem_injury
""")
        
        cur.execute("""
    CREATE TABLE IF NOT EXISTS prem_injury.teams(
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE
    );
""")
        
        cur.execute(f"""
    CREATE TABLE IF NOT EXISTS prem_injury.matches(
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        home_team_id   INTEGER NOT NULL REFERENCES prem_injury.teams(id),
        away_team_id   INTEGER NOT NULL REFERENCES prem_injury.teams(id),
        result         CHAR(1) CHECK (result IN ('H', 'A', 'D')),
        xg_home        REAL,
        xg_away        REAL
            
)
""")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS prem_injury.player_details (
                id              SERIAL PRIMARY KEY,
                understat_id    INTEGER UNIQUE NOT NULL,
                name            TEXT NOT NULL,
                position        TEXT,
                team_id         INTEGER NOT NULL REFERENCES prem_injury.teams(id)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS prem_injury.player_stats (
                id              SERIAL PRIMARY KEY,
                understat_id    INTEGER NOT NULL REFERENCES prem_injury.player_details(understat_id),
                team_id         INTEGER NOT NULL REFERENCES prem_injury.teams(id),
                games           INTEGER,
                minutes         INTEGER,
                goals           INTEGER,
                assists         INTEGER,
                xG              REAL,
                xA              REAL,
                CONSTRAINT player_stats_unique UNIQUE (understat_id, team_id)

            );
""")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS prem_injury.injuries (
                id SERIAL PRIMARY KEY,
                player_id INTEGER REFERENCES prem_injury.player_details(id) ON DELETE CASCADE,
                team_id INTEGER REFERENCES prem_injury.teams(id) ON DELETE CASCADE,
                reason TEXT,
                detail TEXT,
                potential_return DATE,
                condition TEXT,
                status TEXT,
                CONSTRAINT injuries_unique UNIQUE (player_id, team_id)

            );

""")

def load_teams(cur):
    cur.execute(
        """
INSERT INTO prem_injury.teams (id, name) VALUES
(1,  'Arsenal'),
(2,  'Aston Villa'),
(3,  'Bournemouth'),
(4,  'Brentford'),
(5,  'Brighton & Hove Albion'),
(6,  'Chelsea'),
(7,  'Crystal Palace'),
(8,  'Everton'),
(9,  'Fulham'),
(10, 'Ipswich Town'),
(11, 'Leicester City'),
(12, 'Liverpool'),
(13, 'Manchester City'),
(14, 'Manchester United'),
(15, 'Newcastle United'),
(16, 'Nottingham Forest'),
(17, 'Southampton'),
(18, 'Tottenham Hotspur'),
(19, 'West Ham United'),
(20, 'Wolverhampton Wanderers')
ON CONFLICT (id)
DO UPDATE SET name = EXCLUDED.name;
"""
    )

def load_injuries_data(dbconn, injury_rows):
    with dbconn.cursor() as cur:
        for row in injury_rows:
            cur.execute("""
                INSERT INTO prem_injury.injuries (player_id, team_id, reason, detail, potential_return, condition, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id, team_id) DO UPDATE
                SET reason = EXCLUDED.reason,
                    detail = EXCLUDED.detail,
                    potential_return = EXCLUDED.potential_return,
                    condition = EXCLUDED.condition,
                    status = EXCLUDED.status;
            """, (
                row["player_id"],
                row["team_id"],
                row["reason"],
                row["detail"],
                row["potential_return"],
                row["condition"],
                row["status"]
            ))

        dbconn.commit()
        print(f"Inserted {len(injury_rows)} injuries.")

def insert_matches(cur, match_data: list):
    for match in match_data:
            cur.execute("""
                INSERT INTO prem_injury.matches (id, date, home_team_id, away_team_id, result, xg_home, xg_away)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                match["match_id"],
                match["date"],
                match["home_team_id"],
                match["away_team_id"],
                match["result"],
                match["xg_home"],
                match["xg_away"]
            ))
        
    print(f"Inserted {len(match_data)} matches.")

def insert_player_details(cur, players: list[dict]):
    for player in players:
        cur.execute("""
            INSERT INTO prem_injury.player_details (understat_id, name, position, team_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (understat_id) DO NOTHING;
        """, (
            player["understat_id"],
            player["name"],
            player["position"],
            player["team_id"]
        ))
    print(f" Inserted {len(players)} players.")

def insert_player_stats(cur, player_stats: list):
    for stats in player_stats:
        cur.execute("""
            INSERT INTO prem_injury.player_stats (
                understat_id, team_id, games, minutes, goals, assists,
                xG, xA
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (understat_id, team_id) DO UPDATE
            SET games   = EXCLUDED.games,
                minutes = EXCLUDED.minutes,
                goals   = EXCLUDED.goals,
                assists = EXCLUDED.assists,
                xG      = EXCLUDED.xG,
                xA      = EXCLUDED.xA;
        """, (
            stats["understat_id"],
            stats["team_id"],
            stats["games"],
            stats["minutes"],
            stats["goals"],
            stats["assists"],
            stats["xG"],
            stats["xA"],
        ))
        
    print(f"Inserted {len(player_stats)} player stat rows.")
