"""Append-only event recording for Firestore.

Additive, idempotent event logging used for audit trails and cross-process
deduplication. This module is purely additive — it does not modify any existing
``gcp_actions`` behaviour.
"""

from __future__ import annotations

import logging
from typing import Any

from gcp_actions.client import get_any_client

logger = logging.getLogger(__name__)


def _is_duplicate_error(exc: Exception) -> bool:
    """Return True when *exc* signals that the document already exists.

    Inspects the exception class name and MRO instead of importing a
    version-specific exception class, so this works across
    ``google-cloud-firestore`` releases (``Conflict`` / ``AlreadyExists``).
    """
    mro_names = {c.__name__.lower() for c in type(exc).__mro__}
    return bool({"conflict", "alreadyexists"} & mro_names)


def record_event(collection_name: str, doc_id: str, data: dict[str, Any]) -> bool:
    """Atomically create an event document; return True only if newly created.

    Uses :meth:`DocumentReference.create`, which fails atomically when a document
    with the same id already exists. This is the idempotency primitive: the same
    deterministic ``doc_id`` can only ever be written once.

    Args:
        collection_name: Firestore collection to write into.
        doc_id: Deterministic document id (e.g. sha256 of the event key).
        data: Payload to persist.

    Returns:
        True if the document was created (new event), False if it already
        existed (duplicate event).

    Raises:
        Exception: If Firestore client creation or the write fails for any
            reason other than "already exists".
    """
    client = get_any_client("firestore")
    ref = client.collection(collection_name).document(doc_id)
    try:
        ref.create(data)
        return True
    except Exception as exc:
        if _is_duplicate_error(exc):
            logger.info("Duplicate event skipped: %s/%s", collection_name, doc_id)
            return False
        logger.error("record_event failed for %s/%s: %s", collection_name, doc_id, exc)
        raise
