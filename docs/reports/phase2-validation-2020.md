# Phase 2 Validation Report (2020)

- Generated: `2026-03-09T12:43:07`
- DB: `data/mlb_history.db`
- Overall: **FAIL**

## Check Results

### Row coverage vs 2020 games
- Status: **FAIL**
- Summary: games=953, game_team_stats=0/1906 (0.0%), game_pitcher_context=0/1906 (0.0%)
- Details:
```json
{
  "expected_rows_per_table": 1906,
  "game_pitcher_context_coverage": 0.0,
  "game_pitcher_context_rows": 0,
  "game_team_stats_coverage": 0.0,
  "game_team_stats_rows": 0,
  "games": 953,
  "season": 2020
}
```

### Missingness per key feature field
- Status: **FAIL**
- Summary: No rows to evaluate missingness for either table
- Details:
```json
{
  "game_pitcher_context": {
    "career_era": {
      "column_missing": true,
      "null_count": 0,
      "null_pct": 1.0
    },
    "pitcher_id": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 1.0
    },
    "probable_pitcher_id": {
      "column_missing": true,
      "null_count": 0,
      "null_pct": 1.0
    },
    "probable_pitcher_known": {
      "column_missing": true,
      "null_count": 0,
      "null_pct": 1.0
    },
    "season_avg_allowed": {
      "column_missing": true,
      "null_count": 0,
      "null_pct": 1.0
    },
    "season_era": {
      "column_missing": true,
      "null_count": 0,
      "null_pct": 1.0
    },
    "season_runs_per_9": {
      "column_missing": true,
      "null_count": 0,
      "null_pct": 1.0
    },
    "season_stats_leakage_risk": {
      "column_missing": true,
      "null_count": 0,
      "null_pct": 1.0
    },
    "season_strike_pct": {
      "column_missing": true,
      "null_count": 0,
      "null_pct": 1.0
    },
    "season_whip": {
      "column_missing": true,
      "null_count": 0,
      "null_pct": 1.0
    },
    "season_win_pct": {
      "column_missing": true,
      "null_count": 0,
      "null_pct": 1.0
    }
  },
  "game_team_stats": {
    "batting_avg": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 1.0
    },
    "errors": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 1.0
    },
    "hits": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 1.0
    },
    "obp": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 1.0
    },
    "ops": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 1.0
    },
    "runs": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 1.0
    },
    "slg": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 1.0
    },
    "strikeouts": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 1.0
    },
    "walks": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 1.0
    }
  },
  "missing_columns": {
    "game_pitcher_context": [
      "probable_pitcher_id",
      "probable_pitcher_known",
      "season_era",
      "season_whip",
      "season_avg_allowed",
      "season_runs_per_9",
      "season_strike_pct",
      "season_win_pct",
      "career_era",
      "season_stats_leakage_risk"
    ],
    "game_team_stats": []
  }
}
```

### Idempotency checks after rerun
- Status: **PASS**
- Summary: duplicates={'game_team_stats': 0, 'game_pitcher_context': 0}, content_changed={'game_team_stats': False, 'game_pitcher_context': False}
- Details:
```json
{
  "content_changed": {
    "game_pitcher_context": false,
    "game_team_stats": false
  },
  "digests_after": {
    "game_pitcher_context": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "game_team_stats": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
  },
  "digests_before": {
    "game_pitcher_context": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "game_team_stats": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
  },
  "duplicates": {
    "game_pitcher_context": 0,
    "game_team_stats": 0
  },
  "rerun_executed": true,
  "rerun_exit_code": 0,
  "rerun_stderr_tail": "",
  "rerun_stdout_tail": "Backfill complete for season=2020: {\"distinct_games_touched\": 953, \"final_distinct_counts_snapshot\": {\"games\": 953, \"labels\": 951}, \"games_inserted\": 0, \"games_updated\": 953, \"games_upserted\": 1025, \"labels_inserted\": 0, \"labels_updated\": 951, \"labels_upserted\": 951, \"odds_historical\": \"disabled\", \"relevant_rows_processed\": 1025, \"request_count\": 1, \"schedule_rows_fetched\": 1375}\n"
}
```

### Sanity ranges for major numeric fields
- Status: **FAIL**
- Summary: No non-null numeric values present for sanity evaluation
- Details:
```json
{
  "game_pitcher_context": {
    "career_era": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "pitcher_id": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "probable_pitcher_id": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "probable_pitcher_known": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "season_avg_allowed": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "season_era": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "season_runs_per_9": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "season_stats_leakage_risk": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "season_strike_pct": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "season_whip": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "season_win_pct": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    }
  },
  "game_team_stats": {
    "batting_avg": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "errors": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "hits": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "obp": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "ops": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "runs": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "slg": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "strikeouts": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "walks": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    }
  },
  "missing_columns": {
    "game_pitcher_context": [],
    "game_team_stats": []
  }
}
```

### Checkpoint/run observability consistency
- Status: **PASS**
- Summary: checkpoint_status=success, attempts=127, mismatch_keys=[]
- Details:
```json
{
  "checkpoint_cursor": {
    "distinct_games_touched": 953,
    "final_distinct_counts_snapshot": {
      "games": 953,
      "labels": 951
    },
    "games_inserted": 0,
    "games_updated": 953,
    "games_upserted": 1025,
    "labels_inserted": 0,
    "labels_updated": 951,
    "labels_upserted": 951,
    "relevant_rows_processed": 1025,
    "schedule_rows_fetched": 1375,
    "season": 2020
  },
  "mismatches": {},
  "run_note": {
    "distinct_games_touched": 953,
    "final_distinct_counts_snapshot": {
      "games": 953,
      "labels": 951
    },
    "games_inserted": 0,
    "games_updated": 953,
    "games_upserted": 1025,
    "labels_inserted": 0,
    "labels_updated": 951,
    "labels_upserted": 951,
    "odds_historical": "disabled",
    "relevant_rows_processed": 1025,
    "schedule_rows_fetched": 1375
  }
}
```

## Blockers

- Row coverage vs 2020 games: games=953, game_team_stats=0/1906 (0.0%), game_pitcher_context=0/1906 (0.0%)
- Missingness per key feature field: No rows to evaluate missingness for either table
- Sanity ranges for major numeric fields: No non-null numeric values present for sanity evaluation

## Recommendation for 2021

- **NO-GO** until blockers above are resolved.
