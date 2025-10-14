def transform_injury_data(html_soup, dbconn, team_name_to_id=None):
    from datetime import datetime

    # Default to empty dict if not provided
    team_name_to_id = team_name_to_id or {}

    # Step 1: Always initialize player_map
    player_map = {}

    with dbconn.cursor() as cur:
        try:
            cur.execute("SELECT id, name, team_id FROM prem_injury.player_details")
            db_rows = cur.fetchall()
            player_map = {(name.lower(), team_id): player_id for player_id, name, team_id in db_rows}
        except Exception as e:
            print(f"Could not fetch player_details: {e}")
            # leave player_map as {}

    transformed = []

    table = html_soup.find('table', class_='injury-table injury-table-full')
    if not table:
        print("No injury table found.")
        return transformed

    rows = table.find_all('tr')
    current_team = None

    for row in rows:
        class_list = row.get("class", [])

        if "heading" in class_list:
            current_team = extract_team_name(row)
            continue

        if should_skip_row(class_list) or not current_team:
            continue

        if "player-row" in class_list:
            injury = extract_player_injury(row)
            team_id = team_name_to_id.get(current_team)

            if not team_id:
                print(f"Unknown team: {current_team}")
                continue

            player_name = injury.get("Player", "").lower()
            player_id = player_map.get((player_name, team_id))
            if not player_id:
                print(f"Unknown player: {player_name} ({current_team})")
                continue

            return_date = parse_date(injury.get("Potential Return"))

            transformed.append({
                "player_id": player_id,
                "team_id": team_id,
                "reason": injury.get("Reason"),
                "detail": injury.get("Further Detail"),
                "potential_return": return_date,
                "condition": injury.get("Condition"),
                "status": injury.get("Status")
            })

    return transformed



def clean_and_extract(rows):
    teams_injuries = {}
    current_team = None

    for row in rows:
        class_list = row.get("class", [])

        if "heading" in class_list:
            current_team = extract_team_name(row)
            if current_team:
                teams_injuries[current_team] = []
            continue

        if should_skip_row(class_list):
            continue

        if "player-row" in class_list:
            injury_data = extract_player_injury(row)
            if injury_data and current_team:
                teams_injuries[current_team].append(injury_data)

    return teams_injuries


def extract_team_name(row):
    div = row.find("div", class_="injury-team")
    return div.get_text(strip=True) if div else None


def should_skip_row(class_list):
    if not class_list:
        return True
    skip_keywords = ['sub-head', 'team-ad-slot']
    return any(cls in skip_keywords for cls in class_list) or any("showTeam" in cls for cls in class_list)


def extract_player_injury(row):
    cells = row.find_all("td")
    if len(cells) < 6:
        return None  # Guard in case layout is incomplete

    def strip_prefix(text, prefix):
        if text.startswith(prefix) and len(text) > len(prefix):
            return text[len(prefix):].strip()
        return text

    # Clean each cell and remove field prefixes
    player = strip_prefix(clean_text(cells[0].get_text()), "Player")
    reason = strip_prefix(clean_text(cells[1].get_text()), "Reason")
    detail = strip_prefix(clean_text(cells[2].get_text()), "Further Detail")
    return_date = strip_prefix(clean_text(cells[3].get_text()), "Potential Return")
    condition = strip_prefix(clean_text(cells[4].get_text()), "Condition")
    status = strip_prefix(clean_text(cells[5].get_text()), "Status")

    return {
        "Player": player,
        "Reason": reason,
        "Further Detail": detail,
        "Potential Return": return_date,
        "Condition": condition,
        "Status": status
    }

def clean_text(text):
    return ' '.join(text.strip().split()) if text else ''


def parse_date(date_str):
    from datetime import datetime
    if not date_str or "TBC" in date_str:
        return None
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").date()
    except ValueError:
        return None


def transform_match_data(dates_json, team_name_to_id):
    matches = []

    for match in dates_json:
        try:
            match_id = int(match['id'])
            date = match['datetime'][:10]

            home_team = match['h']['title']
            away_team = match['a']['title']
            goals_home = int(match['goals']['h'])
            goals_away = int(match['goals']['a'])

            xg_home = float(match['xG']['h'])
            xg_away = float(match['xG']['a'])

            home_team_id = team_name_to_id.get(home_team)
            away_team_id = team_name_to_id.get(away_team)

            if home_team_id is None or away_team_id is None:
                print(f"Skipping match {match_id}: unknown team(s) → {home_team}, {away_team}")
                continue

            if goals_home > goals_away:
                result = 'H'
            elif goals_home < goals_away:
                result = 'A'
            else:
                result = 'D'

            matches.append({
                'match_id': match_id,
                'date': date,
                'home_team_id': home_team_id,
                'away_team_id': away_team_id,
                'result': result,
                'xg_home': xg_home,
                'xg_away': xg_away
            })
        except Exception as e:
            print(f"Skipping match {match.get('id', 'unknown')}: {e}")
            continue

    return matches

def transform_player_stats(players_data: list[dict], team_name_to_id: dict) -> tuple[list[dict], list[dict]]:
    players, player_stats = [], []

    for p in players_data:
        team_id = team_name_to_id.get(p["team_title"])
        if team_id is None:
            print(f"Unknown team: {p['team_title']} — skipping player {p['player_name']}")
            continue

        players.append({
            "understat_id": int(p["id"]),
            "name": p["player_name"],
            "position": p["position"],
            "team_id": team_id
        })

        player_stats.append({
            "understat_id": int(p["id"]),
            "team_id": team_id,
            "games": int(p["games"]),
            "minutes": int(p["time"]),
            "goals": int(p["goals"]),
            "assists": int(p["assists"]),
            "xg": float(p["xG"]),
            "xa": float(p["xA"]),
            "npxg": float(p["npxG"]),
            "xgchain": float(p["xGChain"]),
            "xgbuildup": float(p["xGBuildup"])
        })

    print(f"Found {len(players)} players")
    return players, player_stats
    
