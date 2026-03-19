# Phase 2 Validation Report (2021)

- Generated: `2026-03-09T19:59:51`
- DB: `/Users/openclaw/.openclaw/projects/predict-mlb/data/mlb_history.db`
- Overall: **PASS**

## Check Results

### Row coverage vs 2021 games
- Status: **PASS**
- Summary: games=2466, completed_games=2466, game_team_stats=4932/4932 (100.0%), game_pitcher_context=4932/4932 (100.0%), feature_rows(v1)=2466/2466 (100.0%)
- Details:
```json
{
  "completed_games": 2466,
  "expected_feature_rows": 2466,
  "expected_game_pitcher_context_rows": 4932,
  "expected_game_team_stats_rows": 4932,
  "feature_rows": 2466,
  "feature_rows_coverage": 1.0,
  "game_pitcher_context_coverage": 1.0,
  "game_pitcher_context_rows": 4932,
  "game_team_stats_coverage": 1.0,
  "game_team_stats_rows": 4932,
  "games": 2466,
  "season": 2021
}
```

### Missingness per key feature field
- Status: **WARN**
- Summary: row_counts(team=4932, pitcher=4932), worst_null_pct=100.0%, missing_columns=[]
- Details:
```json
{
  "game_pitcher_context": {
    "career_era": {
      "column_missing": false,
      "null_count": 4932,
      "null_pct": 1.0
    },
    "pitcher_id": {
      "column_missing": false,
      "null_count": 3,
      "null_pct": 0.0006082725060827251
    },
    "probable_pitcher_id": {
      "column_missing": false,
      "null_count": 3,
      "null_pct": 0.0006082725060827251
    },
    "probable_pitcher_known": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "season_avg_allowed": {
      "column_missing": false,
      "null_count": 312,
      "null_pct": 0.06326034063260341
    },
    "season_era": {
      "column_missing": false,
      "null_count": 312,
      "null_pct": 0.06326034063260341
    },
    "season_runs_per_9": {
      "column_missing": false,
      "null_count": 312,
      "null_pct": 0.06326034063260341
    },
    "season_stats_leakage_risk": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "season_stats_scope": {
      "column_missing": false,
      "null_count": 3,
      "null_pct": 0.0006082725060827251
    },
    "season_strike_pct": {
      "column_missing": false,
      "null_count": 312,
      "null_pct": 0.06326034063260341
    },
    "season_whip": {
      "column_missing": false,
      "null_count": 312,
      "null_pct": 0.06326034063260341
    },
    "season_win_pct": {
      "column_missing": false,
      "null_count": 4932,
      "null_pct": 1.0
    }
  },
  "game_team_stats": {
    "batting_avg": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "errors": {
      "column_missing": false,
      "null_count": 4932,
      "null_pct": 1.0
    },
    "hits": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "obp": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "ops": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "runs": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "slg": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "strikeouts": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "walks": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    }
  },
  "missing_columns": {
    "game_pitcher_context": [],
    "game_team_stats": []
  }
}
```

### Pitcher provenance is parity-safe
- Status: **PASS**
- Summary: rows=4932, non_leaking_rows=4932, safe_scope_rows_for_known_pitchers=4929
- Details:
```json
{
  "non_leaking_rows": 4932,
  "safe_scope_rows_for_known_pitchers": 4929,
  "total_rows": 4932
}
```

### Idempotency checks after rerun
- Status: **PASS**
- Summary: duplicates={'game_team_stats': 0, 'game_pitcher_context': 0, 'feature_rows': 0}, content_changed={'game_team_stats': False, 'game_pitcher_context': False, 'feature_rows': False}
- Details:
```json
{
  "content_changed": {
    "feature_rows": false,
    "game_pitcher_context": false,
    "game_team_stats": false
  },
  "digests_after": {
    "feature_rows": "8635eb87413c4ed111eccbbb73e9ce5599356595873cfd62568ebac3810edd61",
    "game_pitcher_context": "25a02f8149b96d238bd9eb338e086a330532aaf90ee836c438e7d37978488639",
    "game_team_stats": "38b0db30bf0a2a797d97101ffe6e0b05e5efc910d457476490063e97731070e3"
  },
  "digests_before": {
    "feature_rows": "8635eb87413c4ed111eccbbb73e9ce5599356595873cfd62568ebac3810edd61",
    "game_pitcher_context": "25a02f8149b96d238bd9eb338e086a330532aaf90ee836c438e7d37978488639",
    "game_team_stats": "38b0db30bf0a2a797d97101ffe6e0b05e5efc910d457476490063e97731070e3"
  },
  "duplicates": {
    "feature_rows": 0,
    "game_pitcher_context": 0,
    "game_team_stats": 0
  },
  "rerun_executed": false,
  "rerun_exit_code": null,
  "rerun_stderr_tail": "",
  "rerun_stdout_tail": ""
}
```

### Sanity ranges for major numeric fields
- Status: **PASS**
- Summary: total_out_of_range=0
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
      "non_null_count": 4929,
      "out_of_range_count": 0
    },
    "probable_pitcher_id": {
      "column_missing": false,
      "non_null_count": 4929,
      "out_of_range_count": 0
    },
    "probable_pitcher_known": {
      "column_missing": false,
      "non_null_count": 4932,
      "out_of_range_count": 0
    },
    "season_avg_allowed": {
      "column_missing": false,
      "non_null_count": 4620,
      "out_of_range_count": 0
    },
    "season_era": {
      "column_missing": false,
      "non_null_count": 4620,
      "out_of_range_count": 0
    },
    "season_runs_per_9": {
      "column_missing": false,
      "non_null_count": 4620,
      "out_of_range_count": 0
    },
    "season_stats_leakage_risk": {
      "column_missing": false,
      "non_null_count": 4932,
      "out_of_range_count": 0
    },
    "season_strike_pct": {
      "column_missing": false,
      "non_null_count": 4620,
      "out_of_range_count": 0
    },
    "season_whip": {
      "column_missing": false,
      "non_null_count": 4620,
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
      "non_null_count": 4932,
      "out_of_range_count": 0
    },
    "errors": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "hits": {
      "column_missing": false,
      "non_null_count": 4932,
      "out_of_range_count": 0
    },
    "obp": {
      "column_missing": false,
      "non_null_count": 4932,
      "out_of_range_count": 0
    },
    "ops": {
      "column_missing": false,
      "non_null_count": 4932,
      "out_of_range_count": 0
    },
    "runs": {
      "column_missing": false,
      "non_null_count": 4932,
      "out_of_range_count": 0
    },
    "slg": {
      "column_missing": false,
      "non_null_count": 4932,
      "out_of_range_count": 0
    },
    "strikeouts": {
      "column_missing": false,
      "non_null_count": 4932,
      "out_of_range_count": 0
    },
    "walks": {
      "column_missing": false,
      "non_null_count": 4932,
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
- Summary: checkpoint_status=success, attempts=102, mismatch_keys=[]
- Details:
```json
{
  "checkpoint_cursor": {
    "distinct_games_touched": 2466,
    "final_distinct_counts_snapshot": {
      "games": 2466,
      "labels": 2466
    },
    "games_inserted": 2466,
    "games_updated": 0,
    "games_upserted": 2549,
    "labels_inserted": 2466,
    "labels_updated": 0,
    "labels_upserted": 2466,
    "relevant_rows_processed": 2549,
    "schedule_rows_fetched": 2968,
    "season": 2021
  },
  "mismatches": {},
  "run_note": {
    "distinct_games_touched": 2466,
    "final_distinct_counts_snapshot": {
      "games": 2466,
      "labels": 2466
    },
    "games_inserted": 2466,
    "games_updated": 0,
    "games_upserted": 2549,
    "labels_inserted": 2466,
    "labels_updated": 0,
    "labels_upserted": 2466,
    "odds_historical": "disabled",
    "relevant_rows_processed": 2549,
    "schedule_rows_fetched": 2968
  }
}
```

## Blockers

- None

## Recommendation for season 2021

- **GO**
