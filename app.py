from curl_cffi import requests
import pandas as pd
from io import StringIO
import time
from flask import Flask, render_template
from datetime import datetime, timedelta

# --- STEP 1: DEFINE THE RATING MODELS ---
rating_models = {
    "Forward": {
        "base_score": 6.0,
        "weights": {
            "Gls_Performance": 2.0,
            "Ast_Performance": 1.5,
            "xG_Expected": 1.0,
            "SCA_SCA": 0.5,
        }
    },
    "Midfielder": {
        "base_score": 6.0,
        "weights": {
            "Gls_Performance": 0.8,
            "Ast_Performance": 1.0,
            "SCA_SCA": 0.4,
            "Tkl_Tackles": 0.3,
            "Cmp%_Passes": 0.01
        }
    },
    "Defender": {
        "base_score": 6.0,
        "weights": {
            "Tkl_Tackles": 0.6,
            "Blocks_Blocks": 0.5,
            "Int_Defense": 0.4,
            "Clr_Defense": 0.1
        }
    },
    "Goalkeeper": {
        "base_score": 6.0,
        "weights": {
            "GA_Performance": -1.5,
            "Save%_Performance": 0.03,
            "CS%_Performance": 0.02
        }
    }
}

# --- STEP 2: CREATE HELPER FUNCTIONS ---
def get_primary_position(pos_string):
    if not isinstance(pos_string, str): return 'Other'
    if 'GK' in pos_string: return 'Goalkeeper'
    if 'FW' in pos_string: return 'Forward'
    if 'MF' in pos_string: return 'Midfielder'
    if 'DF' in pos_string: return 'Defender'
    return 'Other'

def clean_dataframe_columns(df):
    new_columns = []
    for col_level_1, col_level_2 in df.columns:
        if 'Unnamed' in col_level_1: new_columns.append(col_level_2)
        else: new_columns.append(f"{col_level_2}_{col_level_1}")
    df.columns = new_columns
    return df

def calculate_rating(player_row):
    position = get_primary_position(player_row.get('Pos', ''))
    total_90s = player_row.get('Min_Playing Time', 0) / 90
    if total_90s == 0: return 6.0
    if position not in rating_models: return 6.0
    model = rating_models[position]
    score = model['base_score']
    for stat_name, weight in model['weights'].items():
        total_stat_value = player_row.get(stat_name, 0)
        if position == 'Goalkeeper' and stat_name == 'GA_Performance':
            stat_value_p90 = total_stat_value / total_90s
        elif '%' in stat_name:
            stat_value_p90 = total_stat_value
        else:
            stat_value_p90 = total_stat_value / total_90s
        score += stat_value_p90 * weight
    return round(score, 2)

app = Flask(__name__)

# --- Set up a simple in-memory cache ---
data_cache = {}
CACHE_DURATION = timedelta(hours=4)

def get_team_data(team_name, url):
    """Scrapes, cleans, and calculates ratings for a single team."""
    print(f"\n--- SCRAPING FRESH DATA for {team_name} ---")
    try:
        response = requests.get(url, impersonate="chrome110")
        if response.status_code == 200:
            dfs = pd.read_html(StringIO(response.text))
            if len(dfs) > 0:
                player_stats_df = clean_dataframe_columns(dfs[0])
                defensive_df, sca_df, gk_df = None, None, None
                for df in dfs:
                    if ('Tackles', 'Tkl') in df.columns: defensive_df = clean_dataframe_columns(df)
                    if ('SCA', 'SCA') in df.columns: sca_df = clean_dataframe_columns(df)
                    if ('Performance', 'GA') in df.columns: gk_df = clean_dataframe_columns(df)
                if defensive_df is not None:
                    cols = ['Player', 'Tkl_Tackles', 'Blocks_Blocks', 'Int_Defense', 'Clr_Defense']
                    player_stats_df = pd.merge(player_stats_df, defensive_df[[c for c in cols if c in defensive_df.columns]], on='Player', how='left')
                if sca_df is not None:
                    cols = ['Player', 'SCA_SCA']
                    player_stats_df = pd.merge(player_stats_df, sca_df[[c for c in cols if c in sca_df.columns]], on='Player', how='left')
                if gk_df is not None:
                    cols = ['Player', 'GA_Performance', 'Save%_Performance', 'CS%_Performance']
                    player_stats_df = pd.merge(player_stats_df, gk_df[[c for c in cols if c in gk_df.columns]], on='Player', how='left')
                
                player_stats_df = player_stats_df[~player_stats_df['Player'].str.contains('Total', na=False)]
                player_stats_df['Age'] = player_stats_df['Age'].astype(str).str.split('-').str[0]
                stats_to_convert = [col for col in player_stats_df.columns if 'Tackles' in col or 'Blocks' in col or 'Defense' in col or 'Passes' in col or 'Playing Time' in col or 'Performance' in col or 'Expected' in col or 'SCA' in col]
                for col in stats_to_convert:
                    if col in player_stats_df.columns:
                        player_stats_df[col] = pd.to_numeric(player_stats_df[col], errors='coerce').fillna(0)
                if 'Min_Playing Time' in player_stats_df.columns:
                    player_stats_df = player_stats_df[player_stats_df['Min_Playing Time'] >= 180]
                
                player_stats_df['Calculated_Rating'] = player_stats_df.apply(calculate_rating, axis=1)
                player_stats_df['90s_Played'] = (player_stats_df['Min_Playing Time'] / 90).round(1)
                
                return player_stats_df.sort_values(by='Calculated_Rating', ascending=False)
    except Exception as e:
        print(f"An error occurred while processing {team_name}: {e}")
    return None

team_urls = {
    "Arsenal": "https://fbref.com/en/squads/18bb7c10/Arsenal-Stats",
    "Man City": "https://fbref.com/en/squads/b8fd03ef/Manchester-City-Stats",
    "Liverpool": "https://fbref.com/en/squads/822bd0ba/Liverpool-Stats",
    "Chelsea": "https://fbref.com/en/squads/cff3d9bb/Chelsea-Stats",
    "Man United": "https://fbref.com/en/squads/19538871/Manchester-United-Stats",
    "Tottenham": "https://fbref.com/en/squads/361ca564/Tottenham-Hotspur-Stats"
}

@app.route('/')
def index():
    """The homepage route, which shows links to all teams."""
    return render_template('index.html', teams=team_urls.keys())

@app.route('/team/<team_name>')
def team_page(team_name):
    """The route for a specific team's rating page, now with caching."""
    if team_name in team_urls:
        now = datetime.now()
        if team_name in data_cache and (now - data_cache[team_name]['timestamp']) < CACHE_DURATION:
            print(f"--- Serving CACHED data for {team_name} ---")
            player_data = data_cache[team_name]['data']
        else:
            url = team_urls[team_name]
            player_data = get_team_data(team_name, url)
            if player_data is not None:
                data_cache[team_name] = {'data': player_data, 'timestamp': now}
        
        if player_data is not None:
            players = player_data.to_dict(orient='records')
            return render_template('team_ratings.html', team_name=team_name, players=players)
    return "Team not found or failed to load data.", 404

if __name__ == '__main__':
    app.run(debug=True)

