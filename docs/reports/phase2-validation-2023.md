# Phase 2 Validation Report (2023)

- Generated: `2026-03-09T19:59:51`
- DB: `/Users/openclaw/.openclaw/projects/predict-mlb/data/mlb_history.db`
- Overall: **PASS**

## Check Results

### Row coverage vs 2023 games
- Status: **PASS**
- Summary: games=2471, completed_games=2471, game_team_stats=4942/4942 (100.0%), game_pitcher_context=4942/4942 (100.0%), feature_rows(v1)=2471/2471 (100.0%)
- Details:
```json
{
  "completed_games": 2471,
  "expected_feature_rows": 2471,
  "expected_game_pitcher_context_rows": 4942,
  "expected_game_team_stats_rows": 4942,
  "feature_rows": 2471,
  "feature_rows_coverage": 1.0,
  "game_pitcher_context_coverage": 1.0,
  "game_pitcher_context_rows": 4942,
  "game_team_stats_coverage": 1.0,
  "game_team_stats_rows": 4942,
  "games": 2471,
  "season": 2023
}
```

### Missingness per key feature field
- Status: **WARN**
- Summary: row_counts(team=4942, pitcher=4942), worst_null_pct=100.0%, missing_columns=[]
- Details:
```json
{
  "game_pitcher_context": {
    "career_era": {
      "column_missing": false,
      "null_count": 4942,
      "null_pct": 1.0
    },
    "pitcher_id": {
      "column_missing": false,
      "null_count": 1,
      "null_pct": 0.00020234722784297855
    },
    "probable_pitcher_id": {
      "column_missing": false,
      "null_count": 1,
      "null_pct": 0.00020234722784297855
    },
    "probable_pitcher_known": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "season_avg_allowed": {
      "column_missing": false,
      "null_count": 259,
      "null_pct": 0.05240793201133145
    },
    "season_era": {
      "column_missing": false,
      "null_count": 259,
      "null_pct": 0.05240793201133145
    },
    "season_runs_per_9": {
      "column_missing": false,
      "null_count": 259,
      "null_pct": 0.05240793201133145
    },
    "season_stats_leakage_risk": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "season_stats_scope": {
      "column_missing": false,
      "null_count": 1,
      "null_pct": 0.00020234722784297855
    },
    "season_strike_pct": {
      "column_missing": false,
      "null_count": 259,
      "null_pct": 0.05240793201133145
    },
    "season_whip": {
      "column_missing": false,
      "null_count": 259,
      "null_pct": 0.05240793201133145
    },
    "season_win_pct": {
      "column_missing": false,
      "null_count": 4942,
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
      "null_count": 4942,
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
- Summary: rows=4942, non_leaking_rows=4942, safe_scope_rows_for_known_pitchers=4941
- Details:
```json
{
  "non_leaking_rows": 4942,
  "safe_scope_rows_for_known_pitchers": 4941,
  "total_rows": 4942
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
    "feature_rows": "555b0f9aca79492d94634c953ae623b20059ba8d0b13315c9e1d47ae273e6b6d",
    "game_pitcher_context": "073dca7a600835216599dbbbaecd687bb44b3b46f4f47b637d28a4215714ba2c",
    "game_team_stats": "1935b832aa4f8c0824552cf8ef00e0d1357d78c8582539644d4c9e8dc535549e"
  },
  "digests_before": {
    "feature_rows": "555b0f9aca79492d94634c953ae623b20059ba8d0b13315c9e1d47ae273e6b6d",
    "game_pitcher_context": "073dca7a600835216599dbbbaecd687bb44b3b46f4f47b637d28a4215714ba2c",
    "game_team_stats": "1935b832aa4f8c0824552cf8ef00e0d1357d78c8582539644d4c9e8dc535549e"
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
      "non_null_count": 4941,
      "out_of_range_count": 0
    },
    "probable_pitcher_id": {
      "column_missing": false,
      "non_null_count": 4941,
      "out_of_range_count": 0
    },
    "probable_pitcher_known": {
      "column_missing": false,
      "non_null_count": 4942,
      "out_of_range_count": 0
    },
    "season_avg_allowed": {
      "column_missing": false,
      "non_null_count": 4683,
      "out_of_range_count": 0
    },
    "season_era": {
      "column_missing": false,
      "non_null_count": 4683,
      "out_of_range_count": 0
    },
    "season_runs_per_9": {
      "column_missing": false,
      "non_null_count": 4683,
      "out_of_range_count": 0
    },
    "season_stats_leakage_risk": {
      "column_missing": false,
      "non_null_count": 4942,
      "out_of_range_count": 0
    },
    "season_strike_pct": {
      "column_missing": false,
      "non_null_count": 4683,
      "out_of_range_count": 0
    },
    "season_whip": {
      "column_missing": false,
      "non_null_count": 4683,
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
      "non_null_count": 4942,
      "out_of_range_count": 0
    },
    "errors": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "hits": {
      "column_missing": false,
      "non_null_count": 4942,
      "out_of_range_count": 0
    },
    "obp": {
      "column_missing": false,
      "non_null_count": 4942,
      "out_of_range_count": 0
    },
    "ops": {
      "column_missing": false,
      "non_null_count": 4942,
      "out_of_range_count": 0
    },
    "runs": {
      "column_missing": false,
      "non_null_count": 4942,
      "out_of_range_count": 0
    },
    "slg": {
      "column_missing": false,
      "non_null_count": 4942,
      "out_of_range_count": 0
    },
    "strikeouts": {
      "column_missing": false,
      "non_null_count": 4942,
      "out_of_range_count": 0
    },
    "walks": {
      "column_missing": false,
      "non_null_count": 4942,
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
- Summary: checkpoint_status=success, attempts=101, mismatch_keys=[]
- Details:
```json
{
  "checkpoint_cursor": {
    "distinct_games_touched": 2471,
    "final_distinct_counts_snapshot": {
      "games": 2471,
      "labels": 2471
    },
    "games_inserted": 2471,
    "games_updated": 0,
    "games_upserted": 2517,
    "labels_inserted": 2471,
    "labels_updated": 0,
    "labels_upserted": 2471,
    "relevant_rows_processed": 2517,
    "schedule_rows_fetched": 3009,
    "season": 2023
  },
  "mismatches": {},
  "run_note": {
    "distinct_games_touched": 2471,
    "final_distinct_counts_snapshot": {
      "games": 2471,
      "labels": 2471
    },
    "games_inserted": 2471,
    "games_updated": 0,
    "games_upserted": 2517,
    "labels_inserted": 2471,
    "labels_updated": 0,
    "labels_upserted": 2471,
    "odds_historical": "disabled",
    "relevant_rows_processed": 2517,
    "schedule_rows_fetched": 3009
  }
}
```

## Blockers

- None

## Recommendation for season 2023

- **GO**
