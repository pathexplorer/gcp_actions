# DEVELOP_JOURNAL

Developer log of shared-library fixes. Newest entry first.

---

## 2026-09-11 — `set_firejson` default `merge=None` → `merge=False`

**Repo:** `gcp_actions` (`gcp_actions/firestore_box/json_manipulations.py`)

### What was wrong
- Signature was `set_firejson(self, data_name: dict, merge: bool | None = None)`
  and the body forwarded it positionally: `doc_ref.set(data_name, merge)`.
- The native Firestore signature is `set(document_data, merge=False, ...)`.
  Passing `merge=None` crashes newer `google-cloud-firestore` clients, so every
  caller that relied on the default was broken.
- Only one prod caller used the default:
  `BigBikeData/power_core/.../workshop/instruments.py:299`
  (`download_links` creation). All other callers (markers, cursors, heatmap in
  `get_from_dropbox.py`, `append_function.py`; all `telegram_parcer` writes)
  already passed explicit `True`/`False` and were unaffected.
- `BigBikeData/power_core/tests/test_dropbox_sync.py:222`
  (`test_marker_writes_use_explicit_merge`) already documented the rule:
  "set_firejson must pass explicit merge (None crashes new firestore clients)".

### What we changed (2 files, uncommitted)
1. `gcp_actions/firestore_box/json_manipulations.py:104-112`
   - Default `merge: bool = False` (matches Firestore native default: overwrite).
   - Keyword forward `doc_ref.set(data_name, merge=merge)` instead of positional,
     so future client signature changes can't re-break it.
   - Docstring updated: `False` to overwrite (default), `True` to merge.
2. `CODEBASE_MAP.md:430-432` — same doc correction.

### Why `False` and not `True`
- Restores pre-regression behavior for the single default caller (new uuid doc
  per download link — overwrite and merge are equivalent there, overwrite is
  the Firestore default).
- Zero behavior change for all explicit `True`/`False` callers in BigBikeData
  and `telegram_parcer` (verified by grep before editing).

### Verification
- Mock check against live editable install:
  default call → `set({'a': 1}, merge=False)`,
  explicit `True` → `set({'a': 1}, merge=True)`.
- `BigBikeData/power_core`: 51 passed. `BigBikeData/site_handler`: 4 passed.

### Rollout note
- Local `.venv` installs resolve `gcp_actions` editable from this repo, so dev
  picks the fix up immediately.
- Cloud Run images vendor this repo via `rsync`/`COPY` at build time, so the
  fix reaches dev/prod only on the next image rebuild.
