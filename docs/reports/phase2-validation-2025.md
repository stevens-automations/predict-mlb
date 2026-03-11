# Phase 2 Validation Report (2025)

- Generated: `2026-03-09T19:59:11`
- DB: `/Users/openclaw/.openclaw/workspace/projects/predict-mlb/data/mlb_history.db`
- Overall: **PASS**

## Check Results

### Row coverage vs 2025 games
- Status: **PASS**
- Summary: games=2477, completed_games=2477, game_team_stats=4954/4954 (100.0%), game_pitcher_context=4954/4954 (100.0%), feature_rows(v1)=2477/2477 (100.0%)
- Details:
```json
{
  "completed_games": 2477,
  "expected_feature_rows": 2477,
  "expected_game_pitcher_context_rows": 4954,
  "expected_game_team_stats_rows": 4954,
  "feature_rows": 2477,
  "feature_rows_coverage": 1.0,
  "game_pitcher_context_coverage": 1.0,
  "game_pitcher_context_rows": 4954,
  "game_team_stats_coverage": 1.0,
  "game_team_stats_rows": 4954,
  "games": 2477,
  "season": 2025
}
```

### Missingness per key feature field
- Status: **WARN**
- Summary: row_counts(team=4954, pitcher=4954), worst_null_pct=100.0%, missing_columns=[]
- Details:
```json
{
  "game_pitcher_context": {
    "career_era": {
      "column_missing": false,
      "null_count": 4954,
      "null_pct": 1.0
    },
    "pitcher_id": {
      "column_missing": false,
      "null_count": 5,
      "null_pct": 0.0010092854259184498
    },
    "probable_pitcher_id": {
      "column_missing": false,
      "null_count": 5,
      "null_pct": 0.0010092854259184498
    },
    "probable_pitcher_known": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "season_avg_allowed": {
      "column_missing": false,
      "null_count": 256,
      "null_pct": 0.051675413807024624
    },
    "season_era": {
      "column_missing": false,
      "null_count": 256,
      "null_pct": 0.051675413807024624
    },
    "season_runs_per_9": {
      "column_missing": false,
      "null_count": 256,
      "null_pct": 0.051675413807024624
    },
    "season_stats_leakage_risk": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "season_stats_scope": {
      "column_missing": false,
      "null_count": 5,
      "null_pct": 0.0010092854259184498
    },
    "season_strike_pct": {
      "column_missing": false,
      "null_count": 256,
      "null_pct": 0.051675413807024624
    },
    "season_whip": {
      "column_missing": false,
      "null_count": 256,
      "null_pct": 0.051675413807024624
    },
    "season_win_pct": {
      "column_missing": false,
      "null_count": 4954,
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
      "null_count": 4954,
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
- Summary: rows=4954, non_leaking_rows=4954, safe_scope_rows_for_known_pitchers=4949
- Details:
```json
{
  "non_leaking_rows": 4954,
  "safe_scope_rows_for_known_pitchers": 4949,
  "total_rows": 4954
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
    "feature_rows": "337c7e3cc954674bb703d12df1c3a6d9df0b3f6ee861af80ba746540e9a88afa",
    "game_pitcher_context": "fc0586d710149ef08bf000acf1dae37a8f4521732373fd8790504938235d780f",
    "game_team_stats": "3629385cada06b04cfbeea20349bae2ef7552c6460f775e415590f0230ff7805"
  },
  "digests_before": {
    "feature_rows": "337c7e3cc954674bb703d12df1c3a6d9df0b3f6ee861af80ba746540e9a88afa",
    "game_pitcher_context": "fc0586d710149ef08bf000acf1dae37a8f4521732373fd8790504938235d780f",
    "game_team_stats": "3629385cada06b04cfbeea20349bae2ef7552c6460f775e415590f0230ff7805"
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
      "non_null_count": 4949,
      "out_of_range_count": 0
    },
    "probable_pitcher_id": {
      "column_missing": false,
      "non_null_count": 4949,
      "out_of_range_count": 0
    },
    "probable_pitcher_known": {
      "column_missing": false,
      "non_null_count": 4954,
      "out_of_range_count": 0
    },
    "season_avg_allowed": {
      "column_missing": false,
      "non_null_count": 4698,
      "out_of_range_count": 0
    },
    "season_era": {
      "column_missing": false,
      "non_null_count": 4698,
      "out_of_range_count": 0
    },
    "season_runs_per_9": {
      "column_missing": false,
      "non_null_count": 4698,
      "out_of_range_count": 0
    },
    "season_stats_leakage_risk": {
      "column_missing": false,
      "non_null_count": 4954,
      "out_of_range_count": 0
    },
    "season_strike_pct": {
      "column_missing": false,
      "non_null_count": 4698,
      "out_of_range_count": 0
    },
    "season_whip": {
      "column_missing": false,
      "non_null_count": 4698,
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
      "non_null_count": 4954,
      "out_of_range_count": 0
    },
    "errors": {
      "column_missing": false,
      "non_null_count": 0,
      "out_of_range_count": 0
    },
    "hits": {
      "column_missing": false,
      "non_null_count": 4954,
      "out_of_range_count": 0
    },
    "obp": {
      "column_missing": false,
      "non_null_count": 4954,
      "out_of_range_count": 0
    },
    "ops": {
      "column_missing": false,
      "non_null_count": 4954,
      "out_of_range_count": 0
    },
    "runs": {
      "column_missing": false,
      "non_null_count": 4954,
      "out_of_range_count": 0
    },
    "slg": {
      "column_missing": false,
      "non_null_count": 4954,
      "out_of_range_count": 0
    },
    "strikeouts": {
      "column_missing": false,
      "non_null_count": 4954,
      "out_of_range_count": 0
    },
    "walks": {
      "column_missing": false,
      "non_null_count": 4954,
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
    "distinct_games_touched": 2477,
    "final_distinct_counts_snapshot": {
      "games": 2477,
      "labels": 2477
    },
    "games_inserted": 2477,
    "games_updated": 0,
    "games_upserted": 2511,
    "labels_inserted": 2477,
    "labels_updated": 0,
    "labels_upserted": 2477,
    "relevant_rows_processed": 2511,
    "schedule_rows_fetched": 2995,
    "season": 2025
  },
  "mismatches": {},
  "run_note": {
    "distinct_games_touched": 2477,
    "final_distinct_counts_snapshot": {
      "games": 2477,
      "labels": 2477
    },
    "games_inserted": 2477,
    "games_updated": 0,
    "games_upserted": 2511,
    "labels_inserted": 2477,
    "labels_updated": 0,
    "labels_upserted": 2477,
    "odds_historical": "disabled",
    "relevant_rows_processed": 2511,
    "schedule_rows_fetched": 2995
  }
}
```

## Blockers

- None

## Recommendation for season 2025

- **GO**
