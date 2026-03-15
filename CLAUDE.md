# BetFriend Agent

Card prediction assistant for La Liga + La Liga 2. Sends Telegram analysis ~30 min before each game.

## IMPORTANT: Time Awareness

**Always check the current datetime before answering time-sensitive questions.** Run `date` or use `datetime.now(ZoneInfo("America/Bogota"))` to get the actual current time. Danny is in Bogota (GMT-5). Never assume what day/time it is — always verify. La Liga games are typically:
- Friday: 1 game ~2PM Bogota
- Saturday: 3-4 games ~9AM-3PM Bogota
- Sunday: 3-4 games ~7AM-3PM Bogota
- Monday: 1 game ~2PM Bogota

## Architecture

- **Database:** Supabase free PostgreSQL (project `abwvalvodnzryrygpfmu`, region `us-west-2`, pooler port `6543`)
- **Compute:** GitHub Actions cron workflows (no Docker, no local env, no server)
- **API:** API-Football v3 (`v3.football.api-sports.io`) — free tier 100 req/day, paid Pro for bootstrap only
- **Output:** Telegram bot → chat `-1001510264412`
- **Timezone:** All times in `America/Bogota` (GMT-5)

## GitHub Actions Workflows

| Workflow | Schedule | Purpose |
|----------|----------|---------|
| `daily-fixtures.yml` | `0 5 * * *` (6 AM CET) | Fetch fixtures, send Telegram summary |
| `pre-game-check.yml` | `*/10 10-22 * * *` | Poll for games starting soon, send analysis |
| `bootstrap.yml` | Manual trigger | One-shot DB population (requires paid API tier) |
| `post-game.yml` | `0 1 * * *` (planned) | Backfill results, update stats, log accuracy |

## GitHub Secrets Required

| Secret | Description |
|--------|-------------|
| `SUPABASE_DB_URL` | Pooler connection string (port 6543) |
| `API_FOOTBALL_KEY` | API-Football v3 key |
| `TELEGRAM_TOKEN` | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Telegram chat/group ID |

## Project Structure

```
betfriend/
  config/settings.py       # pydantic-settings (env vars)
  db/store.py              # asyncpg Store (all tables + queries)
  api/client.py            # API-Football httpx client
  api/budget.py            # daily request counter
  jobs/
    daily_fixtures.py      # morning job
    pre_game_check.py      # polling job
    bootstrap.py           # one-shot DB population
  notifications/
    telegram.py            # Telegram bot client
    formatter.py           # message builder (HTML)
  analysis/                # prediction algorithms (Phase 6)
  scrapers/                # RFEF PDF, lineups, coaches, news (Phase 3-5)
```

## Database Tables

**Core:** competitions, teams, fixtures, api_request_log
**Phase 2:** players, player_card_stats, player_fixture_cards, team_season_stats, team_form
**Planned:** referees, referee_game_stats, fixture_lineups, coaches, news_articles, head2head, predictions

## Key Technical Decisions

- Supabase pooler requires `statement_cache_size=0` and SSL context in asyncpg
- Budget tracker reserves 20 req/day for critical pre-game lineup calls
- Bootstrap is resumable: checks `player_fixture_cards` count to skip already-processed fixtures
- API-Football matchday comes as "Regular Season - 30", parsed to int

## Development Phases

- [x] Phase 1: Foundation + Fixtures + Basic Telegram
- [ ] Phase 2: Bootstrap + Team/Player Card Stats (in progress — bootstrap running)
- [ ] Phase 3: RFEF Referee PDF Scraping
- [ ] Phase 4: Lineups + Per-Player Card Risk
- [ ] Phase 5: Coach + H2H + News Bias
- [ ] Phase 6: BetFriend Pronostic (weighted prediction algorithm)
- [ ] Phase 7: Hardening + Accuracy Tracking

## Spec

Full requirements and example Telegram message format in `betfriendagent.txt`.

## Running gh CLI

```bash
export PATH="/c/Program Files/GitHub CLI:/c/Program Files/Git/cmd:$PATH"
gh workflow run <workflow>.yml --ref main
gh run watch <run-id> --exit-status
```
