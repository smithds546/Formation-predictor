#!/usr/bin/env python3
"""
Ultimate SportMonks Fixtures Fetcher
Automatically discovers and fetches Danish Superliga fixtures with full error handling
"""
import requests
import pandas as pd
import time
import sys
import argparse
import os

# Load API token from environment variable
API_TOKEN = os.getenv("SPORTMONKS_API_TOKEN")
if not API_TOKEN:
    print("❌ Error: SPORTMONKS_API_TOKEN environment variable not set")
    print("   Please set it with: export SPORTMONKS_API_TOKEN='your_token_here'")
    sys.exit(1)

API_BASE = "https://api.sportmonks.com/v3/football"

def print_step(num, msg):
    print(f"\n{'='*70}")
    print(f"Step {num}: {msg}")
    print('='*70)

def find_danish_league():
    """Find the Danish league ID in accessible leagues

    This is more tolerant than the original implementation: it will
    - prefer a league whose name contains "superliga" (but not "play")
    - fall back to other heuristics if necessary
    - print the first few available league names to help debugging
    """
    print("Finding Danish Superliga...")

    try:
        response = requests.get(
            f"{API_BASE}/leagues",
            params={"api_token": API_TOKEN, "per_page": 100},
            timeout=10
        )

        if response.status_code != 200:
            print(f"❌ Error fetching leagues: {response.status_code}")
            try:
                error_data = response.json()
                print(f"   Message: {error_data.get('message', 'Unknown error')}")
            except:
                print(f"   Response: {response.text[:200]}")
            return None

        leagues = response.json().get("data", [])

        # Print available league names to help the user confirm
        if leagues:
            print("Available league names (first 10):")
            names = [l.get('name', '') for l in leagues[:10]]
            for n in names:
                print(f"  - {n}")

        # Strategy:
        # 1) Prefer league names that contain 'superliga' and NOT 'play'
        for league in leagues:
            league_name = (league.get("name", "") or "").lower()
            if "superliga" in league_name and "play" not in league_name:
                league_id = league["id"]
                print(f"✅ Selected: {league.get('name')} (ID: {league_id})")
                return league_id

        # 2) If none found, prefer exact match 'superliga'
        for league in leagues:
            league_name = (league.get("name", "") or "").strip().lower()
            if league_name == "superliga":
                league_id = league["id"]
                print(f"✅ Selected (exact): {league.get('name')} (ID: {league_id})")
                return league_id

        # 3) As a fallback, prefer names that contain 'super' but not 'play'
        for league in leagues:
            league_name = (league.get("name", "") or "").lower()
            if "super" in league_name and "play" not in league_name:
                league_id = league["id"]
                print(f"⚠️  Fallback selected: {league.get('name')} (ID: {league_id})")
                return league_id

        # 4) Last resort: look for any league with 'denmark' in the name
        for league in leagues:
            league_name = (league.get("name", "") or "").lower()
            if "denmark" in league_name:
                league_id = league["id"]
                print(f"⚠️  Fallback (denmark) selected: {league.get('name')} (ID: {league_id})")
                return league_id

        print("❌ Danish Superliga not found in accessible leagues")
        print(f"   Available: {', '.join([l.get('name') for l in leagues[:5]])}")
        return None

    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def find_season(league_id, prefer_current=True):
    """Find a season ID for the given league that has fixtures

    Fetch seasons directly from the league endpoint to get all available seasons.
    Test seasons ordered by starting_at (most recent first). Prefer seasons
    where `is_current` is True if available. Returns the first accessible
    season ID or None.
    """
    print(f"Finding available seasons for league {league_id}...")

    try:
        # Fetch league with its seasons (gets more complete list than /seasons endpoint)
        response = requests.get(
            f"{API_BASE}/leagues/{league_id}",
            params={
                "api_token": API_TOKEN,
                "include": "seasons"
            },
            timeout=10
        )

        if response.status_code != 200:
            print(f"❌ Error fetching league: {response.status_code}")
            error_data = response.json() if response.headers.get('content-type') == 'application/json' else {}
            print(f"   {error_data.get('message', response.text[:200])}")
            return None

        league = response.json().get("data", {})
        seasons = league.get("seasons", [])

        if not seasons:
            print("❌ No seasons found for this league")
            return None

        # Sort by starting_at descending (most recent first)
        seasons = sorted(seasons, key=lambda s: s.get('starting_at', ''), reverse=True)

        # Prefer current season if available, but test others if current isn't accessible
        if prefer_current:
            current_season = None
            other_seasons = []
            for s in seasons:
                if s.get('is_current'):
                    current_season = s
                else:
                    other_seasons.append(s)

            if current_season:
                print(f"Found current season candidate: {current_season.get('name')} (ID: {current_season['id']})")
                seasons = [current_season] + other_seasons

        print(f"Found {len(seasons)} seasons for league {league_id}, testing from most recent...")

        # Test each season
        for season in seasons:
            season_id = season["id"]
            year = season.get("year") or season.get("name") or "?"

            # Use filters=fixtureSeasons:ID which is the correct v3 way and works on Free Tier
            test_response = requests.get(
                f"{API_BASE}/fixtures",
                params={
                    "api_token": API_TOKEN,
                    "filters": f"fixtureSeasons:{season_id}",
                    "per_page": 1,
                    # request scores and scoreboards so we can detect numeric goals when available
                    "include": "participants;scores;formations"
                },
                timeout=10
            )

            if test_response.status_code == 200:
                fixtures = test_response.json().get("data", [])
                print(f"✅ Season {year} (ID: {season_id}) - {len(fixtures)} fixtures available")
                return season_id
            else:
                err = None
                try:
                    err = test_response.json()
                except Exception:
                    err = test_response.text[:200]
                print(f"⚠️  Season {year} (ID: {season_id}) - Not accessible: {test_response.status_code} - {err}")

        print("❌ No accessible seasons found")
        return None

    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def fetch_all_fixtures(season_id, max_pages=0):
    """Fetch all fixtures for a season. If max_pages>0, stop after that many pages."""
    print(f"Fetching fixtures for season {season_id}...")

    fixtures = []
    page = 1
    per_page = 100

    try:
        while True:
            if max_pages and page > max_pages:
                print(f"Reached max_pages limit ({max_pages}). Stopping early.")
                break

            print(f"  Page {page}...", end=" ", flush=True)

            response = requests.get(
                f"{API_BASE}/fixtures",
                params={
                    "api_token": API_TOKEN,
                    "filters": f"fixtureSeasons:{season_id}",
                    # request score-related relations so numeric goals are returned when available
                    "include": "participants;scores;formations",
                    "per_page": per_page,
                    "page": page
                },
                timeout=10
            )

            if response.status_code != 200:
                print(f"\n❌ Error: {response.status_code}")
                error_data = response.json() if response.headers.get('content-type') == 'application/json' else {}
                print(f"   {error_data.get('message', response.text[:200])}")
                return None

            data = response.json()
            batch = data.get("data", [])

            print(f"✓ ({len(batch)} fixtures)")
            fixtures.extend(batch)

            if not data.get("pagination", {}).get("has_more"):
                break

            page += 1
            time.sleep(0.5)  # Rate limiting

        print(f"\n✅ Total fixtures retrieved: {len(fixtures)}")
        return fixtures

    except Exception as e:
        print(f"\n❌ Error: {e}")
        return None

def _extract_goals_from_fixture(fixture):
    """Return (home_goals, away_goals) by trying multiple fields/structures."""
    # Try SportMonks v3 scores structure first
    scores = fixture.get('scores', [])
    if isinstance(scores, list) and scores:
        home_goals = None
        away_goals = None
        for s in scores:
            if s.get('description') == 'CURRENT':
                score_data = s.get('score', {})
                if score_data.get('participant') == 'home':
                    home_goals = score_data.get('goals')
                elif score_data.get('participant') == 'away':
                    away_goals = score_data.get('goals')
        
        if home_goals is not None and away_goals is not None:
            return int(home_goals), int(away_goals)

    # Fallback to original heuristics for safety
    # 1) Direct known fields
    direct_home_keys = ['localteam_score', 'local_team_score', 'home_score', 'home_goals', 'home']
    direct_away_keys = ['visitorteam_score', 'visitor_team_score', 'away_score', 'away_goals', 'away']

    for hk in direct_home_keys:
        if hk in fixture and isinstance(fixture[hk], (int, float)):
            for ak in direct_away_keys:
                if ak in fixture and isinstance(fixture[ak], (int, float)):
                    return int(fixture[hk]), int(fixture[ak])

    # 2) scores dict
    scores = fixture.get('scores') or fixture.get('scores_calculated') or {}
    if isinstance(scores, dict):
        # try common key pairs
        def find_pair(d, keys1, keys2):
            for k1 in keys1:
                for k2 in keys2:
                    if k1 in d and k2 in d and isinstance(d[k1], (int, float)) and isinstance(d[k2], (int, float)):
                        return int(d[k1]), int(d[k2])
            return None

        pair = find_pair(scores, ['home','local','localteam','team1'], ['away','visitor','visitorteam','team2'])
        if pair:
            return pair
        # flatten numeric values in scores dict (pick first two) as last resort
        nums = [v for v in scores.values() if isinstance(v, (int, float))]
        if len(nums) >= 2:
            return int(nums[0]), int(nums[1])

    # 3) scoreboards list
    sbs = fixture.get('scoreboards') or []
    if isinstance(sbs, list) and sbs:
        for sb in sbs:
            # common numeric fields
            for hk in ['score_local','local_score','home_score','home']:
                for ak in ['score_visitor','visitor_score','away_score','away']:
                    if hk in sb and ak in sb and isinstance(sb[hk], (int, float)) and isinstance(sb[ak], (int, float)):
                        return int(sb[hk]), int(sb[ak])
            # sometimes there's a 'score' string like '2-1'
            sc = sb.get('score') or sb.get('value') or sb.get('score_string')
            if isinstance(sc, str) and '-' in sc:
                parts = sc.split('-')
                try:
                    return int(parts[0].strip()), int(parts[1].strip())
                except Exception:
                    pass

    # 4) fixture-level score string
    sc = fixture.get('score') or fixture.get('scores')
    if isinstance(sc, str) and '-' in sc:
        parts = sc.split('-')
        try:
            return int(parts[0].strip()), int(parts[1].strip())
        except Exception:
            pass

    # 4b) try to parse numeric scores from textual result_info (e.g. "3-1")
    import re
    result_text = fixture.get('result_info') or fixture.get('result') or ''
    if isinstance(result_text, str):
        m = re.search(r"(\d+)\s*[-:\u2013]\s*(\d+)", result_text)
        if m:
            try:
                return int(m.group(1)), int(m.group(2))
            except Exception:
                pass

    # 4c) check participants or stats fields for numeric score strings
    # sometimes scores may be embedded in nested text
    def find_score_in_obj(obj):
        if isinstance(obj, dict):
            for k,v in obj.items():
                if isinstance(v, str):
                    m = re.search(r"(\d+)\s*[-:\u2013]\s*(\d+)", v)
                    if m:
                        try:
                            return int(m.group(1)), int(m.group(2))
                        except Exception:
                            pass
                else:
                    res = find_score_in_obj(v)
                    if res:
                        return res
        elif isinstance(obj, list):
            for item in obj:
                res = find_score_in_obj(item)
                if res:
                    return res
        return None

    # look in participants and stats
    for key in ('participants', 'stats', 'events'):
        if key in fixture:
            res = find_score_in_obj(fixture[key])
            if res:
                return res

    # 5) No score found
    return 0, 0


def _fixture_has_numeric_scores(fixture):
    """Return True if a fixture contains numeric goal values anywhere we can use."""
    # Check common direct fields
    for k in ['localteam_score','local_team_score','home_score','home_goals','visitorteam_score','visitor_team_score','away_score','away_goals']:
        if k in fixture and isinstance(fixture[k], (int, float)):
            return True

    # Check scores dict
    scores = fixture.get('scores') or fixture.get('scores_calculated') or {}
    if isinstance(scores, dict):
        for v in scores.values():
            if isinstance(v, (int, float)):
                return True

    # Check scoreboards
    sbs = fixture.get('scoreboards') or []
    if isinstance(sbs, list):
        for sb in sbs:
            for v in sb.values():
                if isinstance(v, (int, float)):
                    return True
            sc = sb.get('score') or sb.get('value')
            if isinstance(sc, str) and '-' in sc:
                parts = sc.split('-')
                try:
                    int(parts[0].strip()); int(parts[1].strip())
                    return True
                except Exception:
                    pass

    # No numeric scores found
    return False


def parse_fixtures(fixtures):
    """Parse API fixture data into a DataFrame"""
    rows = []

    for fixture in fixtures:
        try:
            fixture_id = fixture["id"]
            date = fixture.get("starting_at") or fixture.get('time') or fixture.get('date')

            # Extract goals robustly
            home_goals, away_goals = _extract_goals_from_fixture(fixture)

            participants = fixture.get("participants", [])

            home_team = None
            away_team = None
            home_team_name = None
            away_team_name = None

            for p in participants:
                if p.get("meta", {}).get("location") == "home":
                    home_team = p.get("id")
                    home_team_name = p.get("name")
                elif p.get("meta", {}).get("location") == "away":
                    away_team = p.get("id")
                    away_team_name = p.get("name")

            # Extract formations
            home_formation = None
            away_formation = None
            formations = fixture.get("formations", [])
            if isinstance(formations, list):
                for f in formations:
                    if f.get("location") == "home":
                        home_formation = f.get("formation")
                    elif f.get("location") == "away":
                        away_formation = f.get("formation")

            rows.append({
                "fixture_id": fixture_id,
                "date": date,
                "home_team_id": home_team,
                "home_team_name": home_team_name,
                "away_team_id": away_team,
                "away_team_name": away_team_name,
                "home_goals": home_goals,
                "away_goals": away_goals,
                "home_formation": home_formation,
                "away_formation": away_formation,
                "goal_diff": home_goals - away_goals if (home_goals is not None and away_goals is not None) else None,
                "result": (
                    "H" if home_goals > away_goals else
                    "A" if away_goals > home_goals else
                    "D"
                ) if (home_goals is not None and away_goals is not None) else None
            })
        except Exception as e:
            print(f"⚠️  Error parsing fixture {fixture.get('id')}: {e}")
            continue

    return pd.DataFrame(rows)

def main():
    parser = argparse.ArgumentParser(description='Fetch Danish Superliga fixtures from SportMonks')
    parser.add_argument('--season-id', type=int, nargs='+', help='Override season ID(s) to fetch')
    parser.add_argument('--max-pages', type=int, default=0, help='Max pages to fetch (0 = unlimited)')
    args = parser.parse_args()

    print("\n" + "🚀 "*20)
    print("SportMonks Danish Superliga Fixtures Fetcher")
    print("🚀 "*20)

    # Step 1: Find league
    print_step(1, "Finding Danish Superliga league")
    league_id = find_danish_league()
    if not league_id:
        print("❌ Cannot proceed without valid league")
        return False

    # Step 2: Determine seasons
    season_ids = []
    if args.season_id:
        season_ids = args.season_id
        print_step(2, "Using user-provided season(s)")
        print(f"✅ Using season IDs: {season_ids}")
    else:
        print_step(2, "Finding available season")
        sid = find_season(league_id)
        if sid:
            season_ids = [sid]

    if not season_ids:
        print("❌ Cannot proceed without valid season(s)")
        return False

    # Step 3: Fetch fixtures for all seasons
    print_step(3, f"Fetching fixtures for {len(season_ids)} season(s)")
    all_fixtures = []
    for sid in season_ids:
        print(f"⏳ Fetching fixtures for season ID: {sid}...")
        fixtures = fetch_all_fixtures(sid, max_pages=args.max_pages)
        if fixtures:
            all_fixtures.extend(fixtures)
            print(f"✅ Retrieved {len(fixtures)} fixtures for season {sid}")
        else:
            print(f"⚠️  No fixtures retrieved for season {sid}")

    if not all_fixtures:
        print("❌ No fixtures retrieved from any season")
        return False

    # Step 4: Parse data
    print_step(4, "Parsing and processing data")
    df = parse_fixtures(all_fixtures)

    if df.empty:
        print("❌ No fixtures could be parsed")
        return False

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Step 5: Save
    print_step(5, "Saving results")
    output_file = "danish_superliga_fixtures.csv"
    df.to_csv(output_file, index=False)

    print(f"✅ Saved {len(df)} fixtures to {output_file}")
    print("\nFirst 5 matches:")
    print(df.head())

    print("\n" + "="*70)
    print("✨ Success! Data has been fetched and saved.")
    print("="*70 + "\n")

    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

