import requests
from bs4 import BeautifulSoup
import json
import os

def extract_injury_data():
    payload = {
        'api_key': 'your_api_key',
        'url': 'https://www.premierinjuries.com/injury-table.php',
        'render': 'true' 
    }

    response = requests.get('https://api.scraperapi.com/', params=payload)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Basic validation — check that the table or expected content exists
    if not soup.find("table"):
        raise ValueError("Expected injury table not found in the page HTML.")
    
    return soup

def extract_match_data():
    #Send HTTP request
    url = 'https://understat.com/league/EPL/2024'
    res = requests.get(url)

    #Parse HTML
    soup = BeautifulSoup(res.content, 'lxml')

    #find and loop through <script> tags to find datesData
    script_tags = soup.find_all('script')

    for script in script_tags:
        # Extract JSON string from Javascript
        if 'datesData' in script.text:
            raw_text = script.string
            start = raw_text.find("JSON.parse('") + len("JSON.parse('")
            end = raw_text.find("')", start)
            if start == -1 or end == -1:
                    continue
                    
            json_str = raw_text[start:end]

            # Decode unicode escapes
            decoded_json = json_str.encode('utf8').decode('unicode_escape')

            # Parse JSON
            return json.loads(decoded_json)

    # raise error if no data is found
    raise RuntimeError("Failed to extract match data from Understat.")

def extract_understat_player_stats():
    url = "https://understat.com/league/EPL/2024"
    res = requests.get(url)
    soup = BeautifulSoup(res.content, "lxml")

    script_tag = next(s for s in soup.find_all("script") if "playersData" in s.text)
    start = script_tag.text.find("JSON.parse('") + len("JSON.parse('")
    end = script_tag.text.find("')", start)
    raw_json = script_tag.text[start:end]
    decoded_json = raw_json.encode("utf-8").decode("unicode_escape")
    players_data = json.loads(decoded_json)

    return players_data

    

