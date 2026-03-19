# Phase 2 Validation Report (2022)

- Generated: `2026-03-09T19:59:51`
- DB: `/Users/openclaw/.openclaw/projects/predict-mlb/data/mlb_history.db`
- Overall: **PASS**

## Check Results

### Row coverage vs 2022 games
- Status: **PASS**
- Summary: games=2470, completed_games=2470, game_team_stats=4940/4940 (100.0%), game_pitcher_context=4940/4940 (100.0%), feature_rows(v1)=2470/2470 (100.0%)
- Details:
```json
{
  "completed_games": 2470,
  "expected_feature_rows": 2470,
  "expected_game_pitcher_context_rows": 4940,
  "expected_game_team_stats_rows": 4940,
  "feature_rows": 2470,
  "feature_rows_coverage": 1.0,
  "game_pitcher_context_coverage": 1.0,
  "game_pitcher_context_rows": 4940,
  "game_team_stats_coverage": 1.0,
  "game_team_stats_rows": 4940,
  "games": 2470,
  "season": 2022
}
```

### Missingness per key feature field
- Status: **WARN**
- Summary: row_counts(team=4940, pitcher=4940), worst_null_pct=100.0%, missing_columns=[]
- Details:
```json
{
  "game_pitcher_context": {
    "career_era": {
      "column_missing": false,
      "null_count": 4940,
      "null_pct": 1.0
    },
    "pitcher_id": {
      "column_missing": false,
      "null_count": 8,
      "null_pct": 0.0016194331983805667
    },
    "probable_pitcher_id": {
      "column_missing": false,
      "null_count": 8,
      "null_pct": 0.0016194331983805667
    },
    "probable_pitcher_known": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "season_avg_allowed": {
      "column_missing": false,
      "null_count": 320,
      "null_pct": 0.06477732793522267
    },
    "season_era": {
      "column_missing": false,
      "null_count": 320,
      "null_pct": 0.06477732793522267
    },
    "season_runs_per_9": {
      "column_missing": false,
      "null_count": 320,
      "null_pct": 0.06477732793522267
    },
    "season_stats_leakage_risk": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "season_stats_scope": {
      "column_missing": false,
      "null_count": 1,
      "null_pct": 0.00020242914979757084
    },
    "season_strike_pct": {
      "column_missing": false,
      "null_count": 320,
      "null_pct": 0.06477732793522267
    },
    "season_whip": {
      "column_missing": false,
      "null_count": 320,
      "null_pct": 0.06477732793522267
    },
    "season_win_pct": {
      "column_missing": false,
      "null_count": 4940,
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
      "null_count": 4940,
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
- Summary: rows=4940, non_leaking_rows=4940, safe_scope_rows_for_known_pitchers=4939
- Details:
```json
{
  "non_leaking_rows": 4940,
  "safe_scope_rows_for_known_pitchers": 4939,
  "total_rows": 4940
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
    "feature_rows": "2cd32c73e1d8496e412a9136dadc071a897f003d013ef89f140cc8d145da04be",
    "game_pitcher_context": "bbfd4b6d1a96c325933859caa17a708ae2740ff7e6a1ae54100573bccd7709cc",
    "game_team_stats": "1ba1d4e27ffb2675f09c4770412bb0ccf314a0ac2d58e842c81ccabcb8ee352d"
  },
  "digests_before": {
    "feature_rows": "2cd32c73e1d8496e412a9136dadc071a897f003d013ef89f140cc8d145da04be",
    "game_pitcher_context": "bbfd4b6d1a96c325933859caa17a708ae2740ff7e6a1ae54100573bccd7709cc",
    "game_team_stats": "1ba1d4e27ffb2675f09c4770412bb0ccf314a0ac2d58e842c81ccabcb8ee352d"
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
      "non_null_count": 4932,
      "out_of_range_count": 0
    },
    "probable_pitcher_id": {
      "column_missing": false,
      "non_null_count": 4932,
      "out_of_range_count": 0
    },
    "probable_pitcher_known": {
      "column_missing": false,
      "non_null_count": 4940,
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
      "non_null_count": 4940,
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
      "non_null_count": 4940,
      "out_of_range_count": 0
    },
    "errors": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "hits": {
      "column_missing": false,
      "non_null_count": 4940,
      "out_of_range_count": 0
    },
    "obp": {
      "column_missing": false,
      "non_null_count": 4940,
      "out_of_range_count": 0
    },
    "ops": {
      "column_missing": false,
      "non_null_count": 4940,
      "out_of_range_count": 0
    },
    "runs": {
      "column_missing": false,
      "non_null_count": 4940,
      "out_of_range_count": 0
    },
    "slg": {
      "column_missing": false,
      "non_null_count": 4940,
      "out_of_range_count": 0
    },
    "strikeouts": {
      "column_missing": false,
      "non_null_count": 4940,
      "out_of_range_count": 0
    },
    "walks": {
      "column_missing": false,
      "non_null_count": 4940,
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
    "distinct_games_touched": 2470,
    "final_distinct_counts_snapshot": {
      "games": 2470,
      "labels": 2470
    },
    "games_inserted": 2470,
    "games_updated": 0,
    "games_upserted": 2522,
    "labels_inserted": 2470,
    "labels_updated": 0,
    "labels_upserted": 2470,
    "relevant_rows_processed": 2522,
    "schedule_rows_fetched": 2804,
    "season": 2022
  },
  "mismatches": {},
  "run_note": {
    "distinct_games_touched": 2470,
    "final_distinct_counts_snapshot": {
      "games": 2470,
      "labels": 2470
    },
    "games_inserted": 2470,
    "games_updated": 0,
    "games_upserted": 2522,
    "labels_inserted": 2470,
    "labels_updated": 0,
    "labels_upserted": 2470,
    "odds_historical": "disabled",
    "relevant_rows_processed": 2522,
    "schedule_rows_fetched": 2804
  }
}
```

## Blockers

- None

## Recommendation for season 2022

- **GO**
