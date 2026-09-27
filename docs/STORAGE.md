# Per-card Redis storage — v1.1.0

The published package uses Redis for durable production state and a local JSON file for development. This release changes the Redis layout so opening or refreshing card A reads only A and its selected loader asset. Milestones and initial-send claims/settlements read and compare only A's record, then update that record and small registry version/size counters. The host does not transfer unrelated card history for these operations. The browser still polls every ten seconds while visible and nonterminal, and refreshes on reopen; animation runs locally.

## Layout and coordination

The configured `REDIS_KEY` holds a Redis hash marked `live-task-cards:hash:v2`. Each card and content-addressed loader asset has a separate hash field. Shared metadata contains slots and owner preferences. One key keeps Lua transactions on one Redis shard. The public card schema and URL signing remain unchanged.

Card writes compare the complete previously read record, including the send ledger. An unrelated card write does not cause that comparison to fail. A conflicting write to the same card retries the bounded read/compare operation; the service then enforces the request ID, expected revision, and send-claim rules against the latest record. Lost acknowledgments are reconciled using the original request ID, never by blindly sending a second message.

Create, release/discard, pruning, and loader replacement/reset still read a full registry snapshot because they coordinate slots or multiple records. Their transactions compare the shared version, which every card mutation advances. This prevents a global operation from overwriting a concurrent milestone. Only changed hash fields are written. Aggregate storage remains bounded at 3 MB. Archived-card expiration is still enforced on reads; physical pruning happens during create, release/discard, and loader changes, rather than every progress milestone. File-backed development remains a single locked JSON document.

## Existing Redis installation

An empty Redis namespace initializes the new layout on its first creation. A populated legacy JSON-string namespace remains readable by v1.1.0, but writes return `STORE_MIGRATION_REQUIRED` until an explicit migration. Legacy reads still incur the old full-registry cost during that compatibility window. Card reads never migrate or modify storage.

The installer must:

1. Confirm the actual production origin, storage namespace, and signing secret. Keep them unchanged. Stop/drain old publisher requests and in-flight host operations; preserve and reconcile any uncertain provider send using its saved attempt rather than resending.
2. With the production configuration securely supplied in the operator process environment, invoke `scripts/migrate-redis.mjs` with one argument: a new private backup file path outside the checkout and public/build directories. The parent directory must already exist. Use your existing tooling to supply configuration; this script does not load an environment file automatically.
3. The tool validates the legacy registry, creates the backup exclusively with mode 0600, flushes its exact bytes to disk, and atomically replaces the legacy string with the hash only if the original bytes still match. It never prints card contents or credentials. Preserve the backup securely. Backup failure leaves Redis untouched. A concurrent write returns `MIGRATION_CONFLICT` without modifying that newer value; stop the writer and retry with a different backup path. A lost response requires inspecting the current layout; rerunning on an already migrated hash is a no-op.
4. Deploy v1.1.0 to every host instance that touches this namespace, verify its health version, and reconnect the existing runtime. Verify one active card and one retained historical card at their original URLs, their theme/loader, and send-attempt state. Then resume publishing and perform the authorized end-to-end acceptance sequence.

The one-time migration reads the complete registry but preserves card IDs, revisions, URL paths, loader assets/preferences, history, slot ownership and send records. No new card messages are sent. Old package versions cannot operate against the hash: their Redis GET fails rather than treating it as an empty registry. Do not mix old and new host versions or allow a stale deployment to receive production traffic.

## Rollback and other stores

Do not restore the pre-migration backup over newer writes. If rollback is needed after any v1.1.0 write, stop publishers and take a fresh consistent snapshot with the new adapter before planning a compatible reverse conversion. Keep the hash and original backup until recovery is verified. Ordinary rollback to the v1.0.0 host is incompatible with this Redis layout.

This tool does not migrate Blob. If the actual production host uses Blob, preserve its data and routes and inspect that implementation separately. Do not substitute an empty Redis namespace or assert that this Redis optimization upgrades a Blob deployment. No production migration is performed merely by installing or downloading the release.

## Verification

The normal package suite includes REST-boundary tests for bounded per-card reads/writes, races, duplicate-send protection, history, migration and storage errors. For actual Lua execution, point `REDIS_SERVER_BIN` at an existing Redis server binary and run `tests/redis.test.mjs` with Node's test runner. The fixture starts isolated, persistence-disabled Redis processes on private Unix sockets, uses only test data, and stops them afterward. This tests Redis scripts locally; it does not establish Upstash account configuration or physical-device behavior.
