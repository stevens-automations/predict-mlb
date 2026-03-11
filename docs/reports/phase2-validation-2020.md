# Phase 2 Validation Report (2020)

- Generated: `2026-03-09T19:59:45`
- DB: `/Users/openclaw/.openclaw/workspace/projects/predict-mlb/data/mlb_history.db`
- Overall: **PASS**

## Check Results

### Row coverage vs 2020 games
- Status: **PASS**
- Summary: games=953, completed_games=951, game_team_stats=1902/1902 (100.0%), game_pitcher_context=1906/1906 (100.0%), feature_rows(v1)=953/953 (100.0%)
- Details:
```json
{
  "completed_games": 951,
  "expected_feature_rows": 953,
  "expected_game_pitcher_context_rows": 1906,
  "expected_game_team_stats_rows": 1902,
  "feature_rows": 953,
  "feature_rows_coverage": 1.0,
  "game_pitcher_context_coverage": 1.0,
  "game_pitcher_context_rows": 1906,
  "game_team_stats_coverage": 1.0,
  "game_team_stats_rows": 1902,
  "games": 953,
  "season": 2020
}
```

### Missingness per key feature field
- Status: **WARN**
- Summary: row_counts(team=1902, pitcher=1906), worst_null_pct=100.0%, missing_columns=[]
- Details:
```json
{
  "game_pitcher_context": {
    "career_era": {
      "column_missing": false,
      "null_count": 1906,
      "null_pct": 1.0
    },
    "pitcher_id": {
      "column_missing": false,
      "null_count": 13,
      "null_pct": 0.006820566631689402
    },
    "probable_pitcher_id": {
      "column_missing": false,
      "null_count": 13,
      "null_pct": 0.006820566631689402
    },
    "probable_pitcher_known": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "season_avg_allowed": {
      "column_missing": false,
      "null_count": 1906,
      "null_pct": 1.0
    },
    "season_era": {
      "column_missing": false,
      "null_count": 1906,
      "null_pct": 1.0
    },
    "season_runs_per_9": {
      "column_missing": false,
      "null_count": 1906,
      "null_pct": 1.0
    },
    "season_stats_leakage_risk": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "season_stats_scope": {
      "column_missing": false,
      "null_count": 13,
      "null_pct": 0.006820566631689402
    },
    "season_strike_pct": {
      "column_missing": false,
      "null_count": 1906,
      "null_pct": 1.0
    },
    "season_whip": {
      "column_missing": false,
      "null_count": 1906,
      "null_pct": 1.0
    },
    "season_win_pct": {
      "column_missing": false,
      "null_count": 1906,
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
      "null_count": 1902,
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
- Summary: rows=1906, non_leaking_rows=1906, safe_scope_rows_for_known_pitchers=1893
- Details:
```json
{
  "non_leaking_rows": 1906,
  "safe_scope_rows_for_known_pitchers": 1893,
  "total_rows": 1906
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
    "feature_rows": "85e4ae096aea5b2aacabe3307023af7df062ce287a735f0c8c0e5711ec477fe7",
    "game_pitcher_context": "e216f273907d024f947695b0d3fbf02787fe93b20c4d1c592aeca88a0aef7f8a",
    "game_team_stats": "5e2b6484b0dfb333ba1f02b32d675aa560f0cc9ec86c49a3d3aa17c9e91e23a0"
  },
  "digests_before": {
    "feature_rows": "85e4ae096aea5b2aacabe3307023af7df062ce287a735f0c8c0e5711ec477fe7",
    "game_pitcher_context": "e216f273907d024f947695b0d3fbf02787fe93b20c4d1c592aeca88a0aef7f8a",
    "game_team_stats": "5e2b6484b0dfb333ba1f02b32d675aa560f0cc9ec86c49a3d3aa17c9e91e23a0"
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
      "non_null_count": 1893,
      "out_of_range_count": 0
    },
    "probable_pitcher_id": {
      "column_missing": false,
      "non_null_count": 1893,
      "out_of_range_count": 0
    },
    "probable_pitcher_known": {
      "column_missing": false,
      "non_null_count": 1906,
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
      "non_null_count": 1906,
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
      "non_null_count": 1902,
      "out_of_range_count": 0
    },
    "errors": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "hits": {
      "column_missing": false,
      "non_null_count": 1902,
      "out_of_range_count": 0
    },
    "obp": {
      "column_missing": false,
      "non_null_count": 1902,
      "out_of_range_count": 0
    },
    "ops": {
      "column_missing": false,
      "non_null_count": 1902,
      "out_of_range_count": 0
    },
    "runs": {
      "column_missing": false,
      "non_null_count": 1902,
      "out_of_range_count": 0
    },
    "slg": {
      "column_missing": false,
      "non_null_count": 1902,
      "out_of_range_count": 0
    },
    "strikeouts": {
      "column_missing": false,
      "non_null_count": 1902,
      "out_of_range_count": 0
    },
    "walks": {
      "column_missing": false,
      "non_null_count": 1902,
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

- None

## Recommendation for season 2020

- **GO**
