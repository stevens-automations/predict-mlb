# Phase 2 Validation Report (2024)

- Generated: `2026-03-09T19:48:52`
- DB: `/Users/openclaw/.openclaw/workspace/projects/predict-mlb/data/mlb_history.db`
- Overall: **PASS**

## Check Results

### Row coverage vs 2024 games
- Status: **PASS**
- Summary: games=2472, completed_games=2472, game_team_stats=4944/4944 (100.0%), game_pitcher_context=4944/4944 (100.0%), feature_rows(v1)=2472/2472 (100.0%)
- Details:
```json
{
  "completed_games": 2472,
  "expected_feature_rows": 2472,
  "expected_game_pitcher_context_rows": 4944,
  "expected_game_team_stats_rows": 4944,
  "feature_rows": 2472,
  "feature_rows_coverage": 1.0,
  "game_pitcher_context_coverage": 1.0,
  "game_pitcher_context_rows": 4944,
  "game_team_stats_coverage": 1.0,
  "game_team_stats_rows": 4944,
  "games": 2472,
  "season": 2024
}
```

### Missingness per key feature field
- Status: **WARN**
- Summary: row_counts(team=4944, pitcher=4944), worst_null_pct=100.0%, missing_columns=[]
- Details:
```json
{
  "game_pitcher_context": {
    "career_era": {
      "column_missing": false,
      "null_count": 4944,
      "null_pct": 1.0
    },
    "pitcher_id": {
      "column_missing": false,
      "null_count": 2,
      "null_pct": 0.0004045307443365696
    },
    "probable_pitcher_id": {
      "column_missing": false,
      "null_count": 2,
      "null_pct": 0.0004045307443365696
    },
    "probable_pitcher_known": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "season_avg_allowed": {
      "column_missing": false,
      "null_count": 272,
      "null_pct": 0.05501618122977346
    },
    "season_era": {
      "column_missing": false,
      "null_count": 272,
      "null_pct": 0.05501618122977346
    },
    "season_runs_per_9": {
      "column_missing": false,
      "null_count": 272,
      "null_pct": 0.05501618122977346
    },
    "season_stats_leakage_risk": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "season_stats_scope": {
      "column_missing": false,
      "null_count": 2,
      "null_pct": 0.0004045307443365696
    },
    "season_strike_pct": {
      "column_missing": false,
      "null_count": 272,
      "null_pct": 0.05501618122977346
    },
    "season_whip": {
      "column_missing": false,
      "null_count": 272,
      "null_pct": 0.05501618122977346
    },
    "season_win_pct": {
      "column_missing": false,
      "null_count": 4944,
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
      "null_count": 4944,
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
- Summary: rows=4944, non_leaking_rows=4944, safe_scope_rows_for_known_pitchers=4942
- Details:
```json
{
  "non_leaking_rows": 4944,
  "safe_scope_rows_for_known_pitchers": 4942,
  "total_rows": 4944
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
    "feature_rows": "dca76a14683287398934c48fd909978d63f1ed49d02866cb233ee69f002da250",
    "game_pitcher_context": "55ce1f61e2e4e16bf683702332784677d58cfb3f6e6bd01801d8d802f6ec3f3e",
    "game_team_stats": "c8eb995a1bd006da0723c4e7250a427b0bb0128c4be75105b639eae5d6663535"
  },
  "digests_before": {
    "feature_rows": "dca76a14683287398934c48fd909978d63f1ed49d02866cb233ee69f002da250",
    "game_pitcher_context": "55ce1f61e2e4e16bf683702332784677d58cfb3f6e6bd01801d8d802f6ec3f3e",
    "game_team_stats": "c8eb995a1bd006da0723c4e7250a427b0bb0128c4be75105b639eae5d6663535"
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
      "non_null_count": 4942,
      "out_of_range_count": 0
    },
    "probable_pitcher_id": {
      "column_missing": false,
      "non_null_count": 4942,
      "out_of_range_count": 0
    },
    "probable_pitcher_known": {
      "column_missing": false,
      "non_null_count": 4944,
      "out_of_range_count": 0
    },
    "season_avg_allowed": {
      "column_missing": false,
      "non_null_count": 4672,
      "out_of_range_count": 0
    },
    "season_era": {
      "column_missing": false,
      "non_null_count": 4672,
      "out_of_range_count": 0
    },
    "season_runs_per_9": {
      "column_missing": false,
      "non_null_count": 4672,
      "out_of_range_count": 0
    },
    "season_stats_leakage_risk": {
      "column_missing": false,
      "non_null_count": 4944,
      "out_of_range_count": 0
    },
    "season_strike_pct": {
      "column_missing": false,
      "non_null_count": 4672,
      "out_of_range_count": 0
    },
    "season_whip": {
      "column_missing": false,
      "non_null_count": 4672,
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
      "non_null_count": 4944,
      "out_of_range_count": 0
    },
    "errors": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "hits": {
      "column_missing": false,
      "non_null_count": 4944,
      "out_of_range_count": 0
    },
    "obp": {
      "column_missing": false,
      "non_null_count": 4944,
      "out_of_range_count": 0
    },
    "ops": {
      "column_missing": false,
      "non_null_count": 4944,
      "out_of_range_count": 0
    },
    "runs": {
      "column_missing": false,
      "non_null_count": 4944,
      "out_of_range_count": 0
    },
    "slg": {
      "column_missing": false,
      "non_null_count": 4944,
      "out_of_range_count": 0
    },
    "strikeouts": {
      "column_missing": false,
      "non_null_count": 4944,
      "out_of_range_count": 0
    },
    "walks": {
      "column_missing": false,
      "non_null_count": 4944,
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
    "distinct_games_touched": 2472,
    "final_distinct_counts_snapshot": {
      "games": 2472,
      "labels": 2472
    },
    "games_inserted": 2472,
    "games_updated": 0,
    "games_upserted": 2511,
    "labels_inserted": 2472,
    "labels_updated": 0,
    "labels_upserted": 2472,
    "relevant_rows_processed": 2511,
    "schedule_rows_fetched": 2998,
    "season": 2024
  },
  "mismatches": {},
  "run_note": {
    "distinct_games_touched": 2472,
    "final_distinct_counts_snapshot": {
      "games": 2472,
      "labels": 2472
    },
    "games_inserted": 2472,
    "games_updated": 0,
    "games_upserted": 2511,
    "labels_inserted": 2472,
    "labels_updated": 0,
    "labels_upserted": 2472,
    "odds_historical": "disabled",
    "relevant_rows_processed": 2511,
    "schedule_rows_fetched": 2998
  }
}
```

## Blockers

- None

## Recommendation for season 2024

- **GO**
