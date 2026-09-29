"""
scripts/purge_small_buildings.py — remove one- and two-family-home owners
from the Airtable base.

The CRM sync is additive by design: it never deletes, so the owners it
pushed before the building-size gate existed are still in the base. This is
the one-off retraction. It is a script rather than a sync mode precisely so
it cannot run on a schedule.

What it deletes is the difference between two runs of `load_units()` — the
gate off, then on — so the definition of "too small" is exactly the one in
`ingest.pluto.is_landlord_lot()` and cannot drift from the sync's.

Four safety rules, each of which has to hold before anything is removed:

  1. **Only ORT-flagged Managements.** A record without the Owner Research
     Tool checkbox did not originate here and is never touched, however well
     it matches.
  2. **Never a Management a surviving unit still points at.** Two source
     entities can merge onto one record; if either survives the gate, the
     record stays.
  3. **Contacts only when every Management link is doomed**, and only when
     ORT-flagged. A contact reachable from a surviving Management is somebody
     we still want.
  4. **Addresses only when every Management link is doomed.** The Addresses
     table has no ORT flag, so sole-linkage is the only available evidence
     that the record came from here. Airtable clears links to deleted records
     on its own, so a shared address is safe to leave alone.

Usage:
    python scripts/purge_small_buildings.py            # report, delete nothing
    python scripts/purge_small_buildings.py --apply    # delete
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import structlog

from config import config
from pipeline.airtable_sync import (
    AddressField,
    AirtableClient,
    BaseIndex,
    ContactField,
    MgmtField,
    load_units,
)

log = structlog.get_logger(__name__)


def _dropped_and_kept():
    """The units the size gate removes, and the ones it keeps."""
    if not config.PLUTO_GATE_ENABLED:
        raise SystemExit(
            "PLUTO_GATE_ENABLED is false — with the gate off nothing is "
            "excluded and this script has nothing to purge."
        )
    original = config.PLUTO_GATE_ENABLED
    try:
        config.PLUTO_GATE_ENABLED = False
        every, _ = load_units()
    finally:
        config.PLUTO_GATE_ENABLED = original
    kept, _ = load_units()

    kept_ids = {u.entity_id for u in kept}
    dropped = [u for u in every if u.entity_id not in kept_ids]
    return dropped, kept


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Actually delete. Without this, nothing is written.")
    args = parser.parse_args()

    dropped, kept = _dropped_and_kept()
    print(f"Units: {len(kept) + len(dropped)} total -> {len(kept)} kept, {len(dropped)} below threshold")
    if not dropped:
        print("Nothing to purge.")
        return 0

    with AirtableClient(
        config.AIRTABLE_API_KEY, config.AIRTABLE_BASE_ID, dry_run=not args.apply
    ) as client:
        index = BaseIndex(client)
        index.load()

        # Rule 2: a Management any surviving unit resolves to is off limits.
        protected: set[str] = set()
        for unit in kept:
            mgmt_id, _ = index.find_management(unit)
            if mgmt_id:
                protected.add(mgmt_id)

        doomed: dict[str, str] = {}       # mgmt record id -> its name
        skipped_not_ort: list[tuple[str, str]] = []
        skipped_protected: list[tuple[str, str]] = []
        unmatched: list[str] = []
        for unit in dropped:
            mgmt_id, _ = index.find_management(unit)
            if not mgmt_id:
                unmatched.append(unit.name)
                continue
            fields = index.mgmt_by_id.get(mgmt_id, {})
            name = fields.get(MgmtField.NAME) or mgmt_id
            if mgmt_id in protected:
                skipped_protected.append((unit.name, name))
                continue
            if not fields.get(MgmtField.ORT_FLAG):      # Rule 1
                skipped_not_ort.append((unit.name, name))
                continue
            doomed[mgmt_id] = name

        # Rules 3 and 4 need every link, so re-read both tables in full.
        contacts = client.list_records(
            config.AIRTABLE_CONTACTS_TABLE_ID,
            [ContactField.NAME, ContactField.EMAIL, ContactField.PHONE,
             ContactField.MANAGEMENT, ContactField.ORT_FLAG],
        )
        doomed_contacts: list[tuple[str, str]] = []
        kept_contacts = 0
        for record in contacts:
            fields = record.get("fields", {})
            links = fields.get(ContactField.MANAGEMENT) or []
            if not links or not all(link in doomed for link in links):
                continue
            if not fields.get(ContactField.ORT_FLAG):
                kept_contacts += 1
                continue
            doomed_contacts.append((record["id"], fields.get(ContactField.NAME) or record["id"]))

        addresses = client.list_records(
            config.AIRTABLE_ADDRESS_TABLE_ID,
            [AddressField.ADDRESS, AddressField.MANAGEMENT],
        )
        doomed_addresses: list[tuple[str, str]] = []
        for record in addresses:
            fields = record.get("fields", {})
            links = fields.get(AddressField.MANAGEMENT) or []
            if links and all(link in doomed for link in links):
                doomed_addresses.append(
                    (record["id"], fields.get(AddressField.ADDRESS) or record["id"])
                )

        print(f"\nManagements to delete ({len(doomed)}):")
        for name in sorted(doomed.values()):
            print(f"  {name}")
        print(f"\nContacts to delete ({len(doomed_contacts)}):")
        for _, name in sorted(doomed_contacts, key=lambda x: x[1]):
            print(f"  {name}")
        print(f"\nAddresses to delete ({len(doomed_addresses)}):")
        for _, name in sorted(doomed_addresses, key=lambda x: x[1]):
            print(f"  {name}")

        if unmatched:
            print(f"\nNot in the base, nothing to delete ({len(unmatched)}):")
            for name in sorted(unmatched):
                print(f"  {name}")
        if skipped_protected:
            print(f"\nKEPT — a surviving unit shares this Management ({len(skipped_protected)}):")
            for source, target in sorted(skipped_protected):
                print(f"  {source!r} -> {target!r}")
        if skipped_not_ort:
            print(f"\nKEPT — not created by this tool ({len(skipped_not_ort)}):")
            for source, target in sorted(skipped_not_ort):
                print(f"  {source!r} -> {target!r}")
        if kept_contacts:
            print(f"\nKEPT — {kept_contacts} contact(s) on doomed Managements lack the ORT flag.")

        if not args.apply:
            print("\nDRY RUN — nothing was deleted. Re-run with --apply.")
            return 0

        # Children first so no link briefly dangles.
        n_contacts = client.delete_records(
            config.AIRTABLE_CONTACTS_TABLE_ID, [rid for rid, _ in doomed_contacts]
        )
        n_addresses = client.delete_records(
            config.AIRTABLE_ADDRESS_TABLE_ID, [rid for rid, _ in doomed_addresses]
        )
        n_mgmt = client.delete_records(
            config.AIRTABLE_MANAGEMENT_TABLE_ID, list(doomed)
        )
        print(f"\nDeleted: {n_mgmt} Managements, {n_contacts} Contacts, {n_addresses} Addresses.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
