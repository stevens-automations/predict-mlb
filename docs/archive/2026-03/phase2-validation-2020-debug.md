# Phase 2 Validation Report (2020)

- Generated: `2026-03-09T12:53:58`
- DB: `data/mlb_history.db`
- Overall: **FAIL**

## Check Results

### Row coverage vs 2020 games
- Status: **PASS**
- Summary: games=953, completed_games=951, game_team_stats=1902/1902 (100.0%), game_pitcher_context=1906/1906 (100.0%)
- Details:
```json
{
  "completed_games": 951,
  "expected_game_pitcher_context_rows": 1906,
  "expected_game_team_stats_rows": 1902,
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
      "null_count": 27,
      "null_pct": 0.01416579223504722
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
      "null_count": 27,
      "null_pct": 0.01416579223504722
    },
    "season_era": {
      "column_missing": false,
      "null_count": 27,
      "null_pct": 0.01416579223504722
    },
    "season_runs_per_9": {
      "column_missing": false,
      "null_count": 27,
      "null_pct": 0.01416579223504722
    },
    "season_stats_leakage_risk": {
      "column_missing": false,
      "null_count": 0,
      "null_pct": 0.0
    },
    "season_strike_pct": {
      "column_missing": false,
      "null_count": 27,
      "null_pct": 0.01416579223504722
    },
    "season_whip": {
      "column_missing": false,
      "null_count": 27,
      "null_pct": 0.01416579223504722
    },
    "season_win_pct": {
      "column_missing": false,
      "null_count": 82,
      "null_pct": 0.04302203567681007
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
    "game_pitcher_context": "93f2b1825d695cd370b4031e8f9a6102d41d55b3ffcba51681d3c279e90fac59",
    "game_team_stats": "0313c18b46f9c1284f3c588d74e54d1fb58259c77b27ff0dbb2dab1c852d6bba"
  },
  "digests_before": {
    "game_pitcher_context": "93f2b1825d695cd370b4031e8f9a6102d41d55b3ffcba51681d3c279e90fac59",
    "game_team_stats": "0313c18b46f9c1284f3c588d74e54d1fb58259c77b27ff0dbb2dab1c852d6bba"
  },
  "duplicates": {
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
- Status: **FAIL**
- Summary: total_out_of_range=9
- Details:
```json
{
  "game_pitcher_context": {
    "career_era": {
      "column_missing": false,
      "non_null_count": 1879,
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
      "non_null_count": 1879,
      "out_of_range_count": 0
    },
    "season_era": {
      "column_missing": false,
      "non_null_count": 1879,
      "out_of_range_count": 2
    },
    "season_runs_per_9": {
      "column_missing": false,
      "non_null_count": 1879,
      "out_of_range_count": 7
    },
    "season_stats_leakage_risk": {
      "column_missing": false,
      "non_null_count": 1906,
      "out_of_range_count": 0
    },
    "season_strike_pct": {
      "column_missing": false,
      "non_null_count": 1879,
      "out_of_range_count": 0
    },
    "season_whip": {
      "column_missing": false,
      "non_null_count": 1879,
      "out_of_range_count": 0
    },
    "season_win_pct": {
      "column_missing": false,
      "non_null_count": 1824,
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

- Sanity ranges for major numeric fields: total_out_of_range=9

## Recommendation for 2021

- **NO-GO** until blockers above are resolved.
