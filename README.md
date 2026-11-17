# n8n-pipedrive-to-hubspot-incremental-migration-pipeline

**An idempotent, incremental migration engine from Pipedrive to HubSpot, built in n8n.** It extracts Pipedrive **organizations, persons, deals and activities** through the v2 API. Pipedrive foreign keys become HubSpot associations through persisted **ID mapping tables**, and records are written as **batch upserts keyed by unique `pipedrive_*_id` properties**. Every run produces a **dry-run validation report** before anything is written.

```
Pipedrive v2 (updated_since watermark, cursor pages)
   organizations ─► persons ─► deals ─► activities
         │             │          │          │
         ▼             ▼          ▼          ▼
   normalize + validate ─► read-only HubSpot preflight ─► plan + validation report
                                                            │
                 DRY_RUN ◄──────────────────────────────────┤
                                                            ▼
      batch upsert by pipedrive_*_id (companies · contacts · deals · calls/meetings/tasks)
                                                            ▼
      v4 default associations from the ID mapping tables ─► commit mapping tables + watermarks
```

> **Safe by default.** Out of the box the workflow runs in `SANDBOX_MODE=true` against a synthetic Pipedrive fixture: **zero** network calls, nothing written, n8n static data untouched. Going live is staged. First comes a read-only live dry run (Pipedrive extract plus a read-only HubSpot preflight). HubSpot writes happen only after `DRY_RUN=false` **and** `HUBSPOT_WRITES_ENABLED=true`.

---

## Contents

- [Why this design](#why-this-design)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [HubSpot setup: unique ID properties](#hubspot-setup-unique-id-properties)
- [Quick start (sandbox, 2 minutes)](#quick-start-sandbox-2-minutes)
- [Execution steps: going live](#execution-steps-going-live)
- [Environment variables](#environment-variables)
- [Field mapping](#field-mapping)
- [Idempotency, ID mapping and incremental sync](#idempotency-id-mapping-and-incremental-sync)
- [Validation report and quarantine](#validation-report-and-quarantine)
- [Error handling](#error-handling)
- [Offline simulator](#offline-simulator)
- [Troubleshooting](#troubleshooting)
- [Repository layout](#repository-layout)
- [Disclaimer](#️-disclaimer--liability-limitation)

---

## Why this design

| Problem in CRM migrations | How this engine handles it |
|---|---|
| Re-runs or crashes create duplicates | Every write is a HubSpot **batch upsert** keyed by a **unique** `pipedrive_*_id` property. Re-sending a batch updates the same record. A run whose state commit was lost is safe to repeat (tested). |
| Foreign keys break across systems | Pipedrive `org_id` / `person_id` / `deal_id` are resolved through a chain: this run's write results → the persisted **ID mapping table** → a read-only HubSpot lookup by unique property. Pairs that can't be resolved yet become **pending associations** and are retried automatically. |
| Big-bang migrations are risky | **Incremental**: each entity has a watermark (`update_time`), and runs pull only changed records, sorted by `update_time`. A page cap turns a large backlog into consistent chunks. |
| Nobody sees problems until it's too late | The **dry-run validation report** lists planned creates and updates, unchanged skips, quarantined records with reasons, email conflicts, unresolved foreign keys and proposed watermarks, before anything is written. |
| Unchanged records get rewritten on every run | A content hash per record in the mapping table: identical records are `SKIP_UNCHANGED`. |
| Bad data or bad mapping pollutes HubSpot | Validation errors **quarantine** records (never written). Unmapped deal stages are never guessed. Run guards block writes when too many records are quarantined or a run would write too much. |

---

## Architecture

![n8n Pipedrive to HubSpot Incremental Migration Pipeline Overview](https://raw.githubusercontent.com/dhrobinsystems/n8n-pipedrive-to-hubspot-incremental-migration-pipeline/main/workflow-overview.png)

```mermaid
flowchart TD
    T1[Manual Test Trigger] --> CFG
    T2[Incremental Sync hourly :17] --> CFG
    CFG[Config & Safety Gate<br/>flags · host allowlists · mapping tables · guards] --> SB{Sandbox Mode?}
    SB -- yes --> FX[Load Synthetic Pipedrive Fixture]
    SB -- no --> ST[Load Migration State<br/>watermarks · ID maps · pending · fail closed]
    ST --> PO[Pipedrive: Fetch Organizations] --> CO[Collect Organization Pages]
    CO --> PP[Pipedrive: Fetch Persons] --> CP[Collect Person Pages]
    CP --> PDL[Pipedrive: Fetch Deals] --> CD[Collect Deal Pages]
    CD --> PA[Pipedrive: Fetch Activities] --> CA[Collect Activity Pages]
    FX --> NV[Normalize & Validate<br/>HubSpot payloads · issues · hashes]
    CA --> NV
    NV --> BL[Build Preflight Lookups] --> LN{Lookups Needed?}
    LN -- yes --> SL{Sandbox Lookup?}
    SL -- yes --> SIM[Simulated HubSpot Lookup] --> CPR
    SL -- no --> HR[HubSpot: Preflight Batch Read<br/>read-only · fail closed] --> CPR
    LN -- no --> CPR[Collect Preflight Results]
    CPR --> PL[Migration Plan & Validation Report<br/>CREATE · UPDATE · SKIP · QUARANTINE · guards]
    PL --> WA{Write Allowed?}
    WA -- no --> DR[Dry-Run Validation Report] --> RPT
    WA -- yes --> BU[Build Upsert Batches] --> UN{Upserts Needed?}
    UN -- yes --> HU[HubSpot: Batch Upsert] --> RW[Record Write Results] --> BA
    UN -- no --> BA[Build Association Batches<br/>mapping chain · pending queue]
    BA --> AN{Associations Needed?}
    AN -- yes --> HA[HubSpot: Batch Associate] --> RA[Record Association Results] --> CM
    AN -- no --> CM[Commit Migration State<br/>maps · watermarks · pending · quarantine]
    CM --> RPT[Migration Run Report]
```

35 nodes: 2 triggers, 1 sticky note, 19 Code nodes, 6 IF nodes and 7 HTTP Request nodes (4 read-only Pipedrive GETs, 1 read-only HubSpot preflight, and 2 HubSpot write nodes: upsert and associate). Pipedrive is **never written to**.

---

## Prerequisites

- **n8n** 1.x or later (tested on n8n 2.47.7, self-hosted). Core nodes only; no community nodes and no stored n8n credentials.
- `N8N_BLOCK_ENV_ACCESS_IN_NODE=false`, so `{{ $env.X }}` resolves. If env access is blocked, the workflow detects it and stays in sandbox mode.
- **Pipedrive API token** (*Settings → Personal preferences → API*) for a user who can see every record you want to migrate. It is sent only as the `x-api-token` header.
- **HubSpot private app** with:
  - read and write access to companies, contacts and deals (`crm.objects.companies.*`, `crm.objects.contacts.*`, `crm.objects.deals.*`);
  - the read and write scopes HubSpot lists for **calls, meetings and tasks** in the private-app scope picker, if you migrate activities;
  - schema / property write scopes only for the one-time property setup below (you can use a separate token for that).
- **The HubSpot target configured first**: pipelines and deal stages created, users invited (for owner mapping), and the four unique ID properties created (next section).
- **A HubSpot sandbox or developer test account** for the first full rehearsal.

---

## HubSpot setup: unique ID properties

Idempotency depends on four **unique-value** custom properties, one per object type. HubSpot allows up to ten unique-ID properties per object, created with `hasUniqueValue: true`.

| Object type(s) | Property (default name) | Env override |
|---|---|---|
| `companies` | `pipedrive_org_id` | `HS_ORG_ID_PROPERTY` |
| `contacts` | `pipedrive_person_id` | `HS_PERSON_ID_PROPERTY` |
| `deals` | `pipedrive_deal_id` | `HS_DEAL_ID_PROPERTY` |
| `calls`, `meetings`, `tasks` | `pipedrive_activity_id` | `HS_ACTIVITY_ID_PROPERTY` |

```bash
# One request per object type: companies, contacts, deals, calls, meetings, tasks.
# Use an existing property group of that object (GET /crm/v3/properties/<objectType>/groups).
curl -X POST "https://api.hubapi.com/crm/v3/properties/companies" \
  -H "Authorization: Bearer your_hubspot_private_app_token_here" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "pipedrive_org_id",
    "label": "Pipedrive Organization ID",
    "type": "string",
    "fieldType": "text",
    "groupName": "companyinformation",
    "hasUniqueValue": true
  }'
```

> The preflight reads by these properties too, so if a property is missing or not unique, the live dry run fails at the preflight lookup. You find out **before** any write.

---

## Quick start (sandbox, 2 minutes)

```bash
# 1. Run the offline simulator. Zero dependencies, zero network.
node scripts/simulate-sandbox-run.js
#    → 107 checks; validation report, run reports and resulting state in sim-output/

# 2. Import into n8n
#    Workflows → Import from File → workflow.json  (arrives inactive)

# 3. Click "Test workflow" on the Manual Test Trigger.
#    The synthetic fixture runs end to end; open "Dry-Run Validation Report".
```

The fixture is fully synthetic: fictitious companies and people, `example.com` addresses and placeholder IDs. It covers every scenario:

| Records | Scenario | Planned action |
|---|---|---|
| org 9100001 | Migrated earlier with an identical content hash | `SKIP_UNCHANGED` |
| org 9100002 | New; domain read from a website custom field | `CREATE` |
| org 9100003 | No name | `QUARANTINE` (`ORG_MISSING_NAME`) |
| org 9100004 | Already in HubSpot by `pipedrive_org_id`, but not in the local map | `UPDATE` (via preflight) |
| org 9100005 | Older than the watermark | not extracted |
| person 9200003 | Email belongs to a HubSpot contact created outside the migration | `QUARANTINE` (`CONTACT_EMAIL_CONFLICT`) |
| person 9200004 | Same email as another Pipedrive person | `QUARANTINE` (`DUPLICATE_EMAIL_IN_SOURCE`) |
| person 9200006 | Invalid email | `CREATE` without email (warning) |
| person 9200007 | Organization 9100099 exists nowhere | `CREATE`; association **pending** |
| deal 9300002 | Won | stage from `STAGE_MAP_JSON.won` |
| deal 9300003 | Stage 99 not mapped | `QUARANTINE` (`DEAL_UNMAPPED_STAGE`) |
| deal 9300006 | Deleted | excluded |
| activity 9400002 | Planned call | `CALL` task (HubSpot calls have no "scheduled" status) |
| activity 9400007 | HTML note | task, HTML stripped |
| pending seed | Contact 9200101 waited for org 9100002 | resolved in this run |

Sandbox plan: 16 create, 2 update, 1 unchanged, 5 quarantined (20.8%); 25 associations planned, 2 unresolved.

> In sandbox only, the fixture's example owner map, stage map and domain field stand in for `OWNER_MAP_JSON`, `STAGE_MAP_JSON` and `PIPEDRIVE_ORG_DOMAIN_FIELD` when those aren't set.

---

## Execution steps: going live

| Step | Set | What happens |
|---|---|---|
| 1. Sandbox | *(nothing)* | Synthetic fixture; read the validation report. |
| 2. Configure mapping | `OWNER_MAP_JSON`, `STAGE_MAP_JSON`, `ACTIVITY_TYPE_MAP_JSON`, `PIPEDRIVE_ORG_DOMAIN_FIELD` | Pipedrive user IDs → HubSpot owner IDs; Pipedrive stage IDs (plus `won` / `lost`) → HubSpot pipeline and deal-stage internal names. |
| 3. Live dry run | `SANDBOX_MODE=false`, `PIPEDRIVE_API_TOKEN`, `HUBSPOT_PRIVATE_APP_TOKEN` | Read-only Pipedrive extract and read-only HubSpot preflight. **Nothing is written; watermarks don't move.** Iterate until the report looks right. |
| 4. Rehearsal | `DRY_RUN=false`, `HUBSPOT_WRITES_ENABLED=true` against a **HubSpot sandbox** | Full migration; spot-check records and associations. |
| 5. Production backfill | Same, against production, after a HubSpot export | Run manually. Large accounts migrate in chunks (`PIPEDRIVE_MAX_PAGES_PER_ENTITY`), and each run continues from the watermark. |
| 6. Incremental sync | Activate the workflow | Hourly at minute 17: only changed records, with pending associations retried. |
| 7. Cut-over | Freeze Pipedrive edits, run once more, deactivate | Final delta, then switch users to HubSpot. |

### Sample `.env` for step 3

```bash
SANDBOX_MODE=false
DRY_RUN=true
HUBSPOT_WRITES_ENABLED=false
PIPEDRIVE_API_TOKEN=your_pipedrive_api_token_here
HUBSPOT_PRIVATE_APP_TOKEN=your_hubspot_private_app_token_here
MIGRATION_START_DATE=2000-01-01T00:00:00Z
OWNER_MAP_JSON={"1000001":"2000001","1000002":"2000002"}
STAGE_MAP_JSON={"1":{"pipeline":"default","dealstage":"appointmentscheduled"},"2":{"pipeline":"default","dealstage":"qualifiedtobuy"},"won":{"pipeline":"default","dealstage":"closedwon"},"lost":{"pipeline":"default","dealstage":"closedlost"}}
```

---

## Environment variables

All values are read only from `$env`. The complete template with safe defaults is in [.env.example](.env.example).

### Safety switches

| Variable | Default | Turns on/off when | Effect |
|---|---|---|---|
| `SANDBOX_MODE` | `true` | *off* only on `false/0/no/off` | Synthetic fixture, zero network calls |
| `DRY_RUN` | `true` | *off* only on `false/0/no/off` | Validation report only; no writes, no state change |
| `HUBSPOT_WRITES_ENABLED` | `false` | `true/1/yes/on` | Allows HubSpot upserts and associations |
| `ASSOCIATIONS_ENABLED` | `true` | *off* only on `false/0/no/off` | Creates associations from Pipedrive foreign keys |
| `FULL_RESYNC` | `false` | `true/1/yes/on` | Ignores watermarks and hashes; re-extracts from `MIGRATION_START_DATE` (idempotent) |
| `STATE_RESET` | `false` | `true/1/yes/on` | Replaces **corrupt** state with fresh state (for one run) |

A typo (for example `DRY_RUN=flase`) keeps the safe value.

### Connection, scope and batching

| Variable | Default | Notes |
|---|---|---|
| `PIPEDRIVE_API_TOKEN` | `your_pipedrive_api_token_here` | Secret; `x-api-token` header only. |
| `HUBSPOT_PRIVATE_APP_TOKEN` | `your_hubspot_private_app_token_here` | Secret; `Authorization: Bearer` header only. |
| `PIPEDRIVE_API_BASE_URL` / `PIPEDRIVE_ALLOWED_HOSTS` | `https://api.pipedrive.com` / `api.pipedrive.com,.pipedrive.com` | Base URL host must be allowlisted; https only; no path or credentials. |
| `HUBSPOT_API_BASE_URL` / `HUBSPOT_ALLOWED_HOSTS` | `https://api.hubapi.com` / `api.hubapi.com,api-eu1.hubapi.com` | Same rules. |
| `ENTITIES` | all four | Always processed in dependency order. |
| `MIGRATION_START_DATE` | `2000-01-01T00:00:00Z` | First-run lower bound (RFC3339 UTC). |
| `DEAL_STATUSES` | `open,won,lost` | Deleted deals are never migrated. |
| `PIPEDRIVE_PAGE_SIZE` | `500` | Pipedrive v2 maximum is 500. |
| `PIPEDRIVE_MAX_PAGES_PER_ENTITY` | `20` | Up to 10,000 records per entity per run; the rest continues next run. |
| `BATCH_SIZE` / `ASSOCIATION_BATCH_SIZE` | `100` / `100` | HubSpot batch read / upsert limit is 100. |

### Mapping, policies and guards

| Variable | Default | Notes |
|---|---|---|
| `OWNER_MAP_JSON` | `{}` | `{"<pipedrive user id>":"<hubspot owner id>"}`. Unmapped → owner blank (warning). |
| `STAGE_MAP_JSON` | `{}` | `{"<stage id>"|"won"|"lost": {"pipeline":"…","dealstage":"…"}}`. `won` / `lost` take precedence for closed deals. |
| `DEFAULT_DEAL_PIPELINE` / `DEFAULT_DEAL_STAGE` | *(empty)* | Optional fallback; otherwise unmapped deals are quarantined. |
| `ACTIVITY_TYPE_MAP_JSON` | `call→calls`, `meeting/lunch→meetings`, `task/deadline/email→tasks` | Unknown types → tasks (warning). |
| `PIPEDRIVE_ORG_DOMAIN_FIELD` | *(empty)* | Custom field key holding the website; normalized to a bare domain. |
| `MAP_DEAL_CURRENCY` | `false` | Writes `deal_currency_code` (requires multi-currency in HubSpot). |
| `CONTACT_EMAIL_CONFLICT_POLICY` | `quarantine` | `quarantine`, `strip_email` (create without email) or `adopt` (tag the existing HubSpot contact with the Pipedrive ID). |
| `HS_*_ID_PROPERTY` | `pipedrive_*_id` | The unique properties used as upsert keys. |
| `MAX_WRITES_PER_RUN` | `5000` | Over the limit → writes blocked (`BLOCKED_BY_GUARD`). |
| `MAX_QUARANTINE_PCT` | `25` | Over the limit → writes blocked (usually a mapping problem). |
| `MAX_PENDING_ASSOCIATIONS` | `10000` | Queue cap; the oldest entries are dropped and reported. |
| `NOW_OVERRIDE` | — | Pins "now" for reproducible tests. |

---

## Field mapping

| Pipedrive (v2) | HubSpot | Notes |
|---|---|---|
| **organization** `name` | company `name` | Required, else quarantined. |
| `address.formatted_address` / `value`, `locality`, `country`, `postal_code` | `address`, `city`, `country`, `zip` | |
| custom field `PIPEDRIVE_ORG_DOMAIN_FIELD` | `domain` | `https://www.x.com/path` → `x.com` |
| `owner_id` | `hubspot_owner_id` | via `OWNER_MAP_JSON` |
| `id` | `pipedrive_org_id` | unique upsert key |
| **person** `first_name`, `last_name` (or split `name`) | contact `firstname`, `lastname` | |
| primary `emails[].value` | `email` | lower-cased; invalid → omitted; duplicates / conflicts → policy |
| primary `phones[].value` | `phone` | |
| `org_id` | association contact → company | |
| **deal** `title`, `value` | deal `dealname`, `amount` | invalid amount → omitted (warning) |
| `stage_id` / `status` | `pipeline`, `dealstage` | via `STAGE_MAP_JSON`; never guessed |
| `won_time` / `lost_time` / `close_time` / `expected_close_date` | `closedate` | |
| `org_id`, `person_id` | associations deal → company, deal → contact | |
| **activity** `type` | `calls` / `meetings` / `tasks` | via `ACTIVITY_TYPE_MAP_JSON`; open calls → CALL tasks |
| `subject`, `note` (HTML stripped) | `hs_call_title`/`hs_call_body`, `hs_meeting_title`/`hs_meeting_body`, `hs_task_subject`/`hs_task_body` | |
| `due_date` + `due_time` (else `add_time`) | `hs_timestamp`, meeting start / end (`duration`) | |
| `done` | `hs_call_status`, `hs_meeting_outcome`, `hs_task_status` | |
| `deal_id`, `person_id`, `org_id` | associations activity → deal / contact / company | |

Not migrated by default: notes, files, products / line items, email threads, custom fields (other than the domain field), followers, deleted records and merges. Extend `Normalize & Validate` to add fields. The hash then changes, and the next run updates the affected records.

---

## Idempotency, ID mapping and incremental sync

**Upserts by unique property.** Each batch is `POST /crm/v3/objects/{objectType}/batch/upsert` with `idProperty = pipedrive_*_id` and up to 100 inputs. A retried or repeated batch can't duplicate, because HubSpot enforces the property's uniqueness. With `CONTACT_EMAIL_CONFLICT_POLICY=adopt`, an existing contact found by email is instead updated by its HubSpot ID (`batch/update`).

**ID mapping tables** live in workflow static data (`migration.maps`):

```json
{
  "organizations": { "9100002": { "id": "80000001", "h": "3f1c9a20" } },
  "activities":    { "9400002": { "id": "80000014", "h": "a81e2b07", "t": "tasks" } }
}
```

- `id` is the HubSpot record ID, used to resolve foreign keys.
- `h` is the content hash, used for `SKIP_UNCHANGED`.
- `t` pins an activity's HubSpot object type, so a planned call later marked done stays the same task instead of becoming a second record.

**Foreign-key resolution chain** for every association:

1. This run's write results.
2. The persisted mapping table.
3. The read-only HubSpot preflight (batch read by `pipedrive_*_id`).

If an endpoint still isn't in HubSpot (quarantined, failed, or not yet extracted), the pair goes into `migration.pending` and is retried on every run until it resolves. Associations use `POST /crm/v4/associations/{from}/{to}/batch/associate/default`.

**Watermarks.** Each entity is fetched with `updated_since=<watermark>&sort_by=update_time&sort_direction=asc` and cursor pagination (`additional_data.next_cursor`).

- After a successful run, the watermark moves to the entity's largest `update_time`.
- If **any** write for an entity failed, that entity's watermark is **held**, so the next run retries. The upserts make that safe.
- Because `updated_since` is inclusive, boundary records are re-fetched and skipped as unchanged.
- When `PIPEDRIVE_MAX_PAGES_PER_ENTITY` is hit, the processed prefix is consistent (sorted by `update_time`), `backlogRemaining` is flagged, and the next run continues.

**Crash recovery.** If n8n stops after HubSpot accepted writes but before the state commit, the next run re-extracts the same records. The preflight finds them by `pipedrive_*_id` and classifies them as `UPDATE`, and the upserts touch the same HubSpot records. The simulator tests this explicitly: zero duplicates.

**Persistence.** n8n saves static data for production executions (active workflow, schedule trigger). Avoid overlapping runs: keep the schedule interval longer than a run, or limit production concurrency. Static data grows with the mapping tables (roughly 60–80 bytes per record), so for very large accounts, export the run reports and state periodically.

---

## Validation report and quarantine

The dry-run report (node **Dry-Run Validation Report**, also produced before every live write) contains:

| Section | Content |
|---|---|
| `summary` | Per entity: extracted, CREATE, UPDATE, SKIP_UNCHANGED, QUARANTINE, warnings |
| `totals` | Writes, creates, updates, unchanged, quarantined (and %), associations planned and unresolved |
| `issueCounts` | Count per issue code |
| `quarantine` | Every quarantined record with readable reasons |
| `warnings` | Records written with warnings |
| `emailConflicts` | Duplicate or conflicting emails and the policy applied |
| `fkUnresolved` | Associations whose endpoint isn't in HubSpot (these become pending) |
| `plannedWrites` | Entity, object type, Pipedrive ID, action, HubSpot ID, mode |
| `proposedWatermarks` / `extractedSince` / `backlogRemaining` | Incremental window |
| `guards` | Guard results |

**Issue codes:**

| Severity | Codes |
|---|---|
| Errors (quarantine; never written) | `ORG_MISSING_NAME`, `PERSON_MISSING_NAME_AND_EMAIL`, `DUPLICATE_EMAIL_IN_SOURCE`, `CONTACT_EMAIL_CONFLICT`, `DEAL_MISSING_TITLE`, `DEAL_UNMAPPED_STAGE`, `ACTIVITY_INVALID_DATE`, `ACTIVITY_TYPE_MISMATCH_EXISTING` |
| Warnings (written) | `OWNER_UNMAPPED`, `ORG_INVALID_DOMAIN`, `PERSON_INVALID_EMAIL`, `DEAL_INVALID_AMOUNT`, `DEAL_DEFAULT_STAGE_USED`, `ACTIVITY_TYPE_DEFAULTED`, `ACTIVITY_NO_DUE_DATE`, `ACTIVITY_NO_PARENT`, `CONTACT_EMAIL_STRIPPED`, `CONTACT_ADOPTED_BY_EMAIL`, `TEXT_TRUNCATED` |

Quarantined records are kept in `migration.quarantine` with their reasons. Fix the record in Pipedrive: its `update_time` changes, and the next incremental run picks it up.

---

## Error handling

| Failure | Behavior |
|---|---|
| Pipedrive HTTP error (after 3 tries) or an unsuccessful / malformed page | Run stops before any HubSpot write. |
| Pipedrive pagination stops early with a cursor | Run stops. A page-cap stop is handled as backlog instead. |
| HubSpot preflight lookup fails | Run stops (an incomplete lookup could hide a conflict). |
| Corrupt migration state | Run stops before extraction; `STATE_RESET=true` rebuilds it (safe, because upserts and the preflight prevent duplicates). |
| HubSpot upsert batch fails (after 3 tries) | Records in that batch are marked failed, the entity's watermark is held, the status is `PARTIAL`, and the next run retries. |
| Partial batch response | Only records without a result are failed. |
| Association batch fails | Pairs go back into the pending queue; status `PARTIAL`. |
| Too many writes / too many quarantined | `BLOCKED_BY_GUARD`; nothing is written. |
| Config error (bad JSON map, host not allowlisted, bad batch size, …) | Writes blocked; errors listed in the report. |
| Rate limits (429) | HTTP nodes retry (3 tries, 3 s apart). Requests are spaced (Pipedrive 250 ms between pages, HubSpot 150 ms between batches). |

Run statuses: `SANDBOX_DRY_RUN`, `DRY_RUN`, `NOTHING_TO_MIGRATE`, `BLOCKED_BY_GUARD`, `MIGRATED`, `PARTIAL`.

---

## Offline simulator

```bash
node scripts/simulate-sandbox-run.js          # verbose
node scripts/simulate-sandbox-run.js --quiet  # summary only
```

Zero dependencies (Node ≥ 18, using only `fs`, `path` and `vm`). It executes `workflow.json` itself. It follows connections, runs every Code node in a VM, evaluates expressions, and emulates cursor pagination and static data. HTTP goes to in-memory Pipedrive v2 and HubSpot mocks, and the HubSpot mock **enforces unique `pipedrive_*_id` values and unique contact emails**, so any non-idempotent write path fails. Its **107 checks** cover:

1. Structure and static safety: read-only Pipedrive, token headers via `$env`, only two write builders, license footer in every Code node, no secrets.
2. Safe defaults and config validation: typos, blocked env, missing tokens, host allowlists, invalid maps and policies.
3. Fixture parity and synthetic data.
4. Sandbox end to end: every action, quarantine reason, field mapping, activity rule, report section and deterministic hash.
5. Live dry run: Pipedrive query parameters and auth, read-only preflight, plan parity with the sandbox, no state change.
6. Armed migration: HubSpot counts, in-place updates, every FK association, pending resolution, mapping tables, watermarks, **rerun with zero writes**, **crash recovery with zero duplicates**, incremental change, pinned activity type.
7. Pagination over 1,207 records, backlog chunking, write outage → held watermark → clean retry, association outage, fail-closed extract / preflight / state, `STATE_RESET`.
8. Policies and guards: write and quarantine guards, email-conflict policies, `ENTITIES`, `FULL_RESYNC`, associations off, default stage, nothing-to-migrate, schedule trigger.

Outputs go to `sim-output/` (git-ignored): `sandbox-validation-report.json`, `sandbox-run-report.json`, `armed-run-report.json` and `migration-state-after-run.json`.

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Status is always `DRY_RUN` | `DRY_RUN` is still on, `HUBSPOT_WRITES_ENABLED` isn't `true`, `$env` is blocked, or a config error exists. See `writeBlockedBy` in the run report. |
| `Live mode … refused` | Set `PIPEDRIVE_API_TOKEN` and `HUBSPOT_PRIVATE_APP_TOKEN`. |
| Preflight fails with "not a unique property" / 400 | The `pipedrive_*_id` property is missing or not created with `hasUniqueValue: true` on that object type. |
| Every deal is quarantined | `STAGE_MAP_JSON` is empty or uses the wrong stage IDs. Get the Pipedrive stage IDs from `/api/v2/stages` and the HubSpot internal stage names from the pipeline settings. |
| `BLOCKED_BY_GUARD` | Read `guards.failures`. Fix the mapping or data, or raise `MAX_QUARANTINE_PCT` / `MAX_WRITES_PER_RUN` deliberately. |
| `CONTACT_EMAIL_CONFLICT` | The email already belongs to a HubSpot contact. Choose `CONTACT_EMAIL_CONFLICT_POLICY=adopt` or `strip_email`, or merge the records first. |
| Associations stay pending | The target is quarantined, failed, or outside the migrated scope. See `fkUnresolved` and `migration.pending`. |
| `PARTIAL` | Some batches failed. The watermark is held, so just run again; upserts make it safe. |
| Migration state "failed validation" | Static data was edited or corrupted. Inspect it, or set `STATE_RESET=true` for one run. |
| Watermarks never advance on manual test runs | Static data persists for production executions; activate the workflow or use the schedule. |

---

## Repository layout

```
n8n-pipedrive-to-hubspot-incremental-migration-pipeline/
├── workflow.json                     # n8n workflow (35 nodes, inactive on import)
├── README.md                         # this file
├── DISCLAIMER.md                     # liability limitation
├── LICENSE                           # MIT
├── .env.example                      # every variable, placeholder values, safe defaults
├── .gitignore                        # .env, SQLite / n8n data, sandbox outputs, exports
├── sample-payloads/
│   └── pipedrive-fixture.json        # synthetic Pipedrive records, HubSpot snapshot, state seed
└── scripts/
    └── simulate-sandbox-run.js       # zero-dependency offline simulator (107 checks)
```

---

## ⚠️ Disclaimer & Liability Limitation

**Use at your own risk.** The code, workflows, and scripts in this repository are provided for educational and architectural demonstration purposes.

**No Liability:** The author and dhRobin Systems accept no responsibility or liability for any direct, indirect, incidental, or consequential damages that result from using, misusing, or deploying these scripts, including data loss, incomplete or duplicated migrations, overwritten HubSpot records and incorrect mappings or associations.

**No Warranty:** All software, n8n JSON blueprints, JavaScript modules, field mappings and sample payloads are provided "AS IS", without warranty of any kind, express or implied, including warranties of merchantability, fitness for a particular purpose and non-infringement. Pipedrive and HubSpot APIs and object models can change at any time.

**Production Testing Required:** Always test thoroughly in an isolated staging environment, such as a HubSpot developer test account or sandbox, with `SANDBOX_MODE=true` and `DRY_RUN=true` enabled, before you connect live Pipedrive or HubSpot credentials or set `HUBSPOT_WRITES_ENABLED=true`. Review every validation report and back up HubSpot before the first live run.

**Third-Party APIs & Compliance:** You alone are responsible for complying with the terms of service, rate limits, data privacy regulations (GDPR/CCPA) and security policies of Pipedrive, HubSpot, n8n and any connected services. That includes confirming a lawful basis for transferring personal data, honouring consent and deletion requests in both systems, and reviewing every automatically created or updated record.

The full disclaimer is also available in [DISCLAIMER.md](DISCLAIMER.md).

## License

Released under the MIT License. Copyright (c) 2026 DH Robin (dhRobin Systems). See [LICENSE](LICENSE).

---

Full text: DISCLAIMER.md · Licensed under the MIT License. License: MIT © 2026 dhRobin Systems · Provided as-is, see the disclaimer.
