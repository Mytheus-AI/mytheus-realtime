"""Compliance-gate condition 3: the `data_residency` pin (plan 3.5 point 3, 3.6 step 4).

`transcription-service` reads `data_residency.region` (owned by `identity-tenant-service` in
`mytheus-backend`, table already migrated in that repo's `82d44163c933`/`b7d9f3a55c21`) to
select the Deepgram regional endpoint. See `tenancy.py` for the read path (direct SQL against
the shared Postgres — see that module's docstring for why a direct read is the sanctioned
pattern here rather than a REST call).

Region-local Postgres/Redpanda cell routing (the second half of condition 3 — "ensure
transcript_chunks and the transcript-chunks topic land in the tenant region") is NOT
implemented: this program has exactly one Postgres and one Redpanda broker in every
environment that exists today (`db_cell` on `data_residency` is a single value,
`'pool-us-1'`, for every tenant; no second regional cell has been provisioned anywhere in the
stack). Endpoint SELECTION is real and tested; physical multi-region placement is
infrastructure that does not exist yet in any repo, so it is out of reach of a code change
here and is reported BLOCKED, not implemented against a fake.
"""

from __future__ import annotations

# Deepgram's actual regional streaming endpoints. `ap` has no dedicated Deepgram region as of
# this writing, so it falls back to the US endpoint (documented, not silently assumed) — a
# residency ADR update would be needed before onboarding an `ap` tenant against Deepgram for
# real; this fallback exists only so `deepgram_endpoint_for_region` is total over the three
# regions `data_residency.region`'s check constraint allows (`us`/`eu`/`ap`).
_REGION_ENDPOINTS = {
    "us": "wss://api.deepgram.com/v1/listen",
    "eu": "wss://api.eu.deepgram.com/v1/listen",
}


def deepgram_endpoint_for_region(region: str) -> str:
    return _REGION_ENDPOINTS.get(region, _REGION_ENDPOINTS["us"])
