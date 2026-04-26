"""
main.py — CLI entry point

Usage:
  python main.py full-load          # One-time initial data load
  python main.py daily              # Run the daily pipeline once
  python main.py weekly             # Run the weekly pipeline once
  python main.py enrich             # Enrichment only (no new ingestion)
  python main.py stats              # Print database stats
  python main.py pierce --entity "123 BROADWAY LLC"  # Pierce a specific LLC
  python main.py pierce --address "123 Broadway, Brooklyn, NY 11201"  # Full pipeline for an address
  python main.py schedule           # Start the persistent scheduler

Examples:
  python main.py full-load
  python main.py pierce --entity "WEST 72 STREET OWNERS CORP"
  python main.py pierce --address "67 Woodhull St, Brooklyn, NY 11231"
  python main.py pierce --address "67 Woodhull St, Brooklyn, NY 11231" --skip-enrichment
  python main.py stats
"""

import asyncio
import sys

import click
import structlog

log = structlog.get_logger(__name__)


@click.group()
def cli():
    """Owner Research Tool — Research Pipeline CLI"""
    pass


@cli.command("full-load")
def full_load():
    """Run the one-time full data load (HPD + ACRIS + enrichment)."""
    click.echo("Starting full data load — this may take several hours for all of NYC.")
    click.echo("Monitor progress in the ingestion_log table in Supabase.\n")

    from pipeline.orchestrator import run_initial_full_load
    asyncio.run(run_initial_full_load())
    click.echo("Full load complete.")


@cli.command("daily")
def daily():
    """Run the daily pipeline (ACRIS delta + enrichment)."""
    from pipeline.orchestrator import run_daily_pipeline
    asyncio.run(run_daily_pipeline())


@cli.command("weekly")
def weekly():
    """Run the weekly pipeline (HPD + WoW + daily pipeline)."""
    from pipeline.orchestrator import run_weekly_pipeline
    asyncio.run(run_weekly_pipeline())


@cli.command("enrich")
def enrich():
    """Run enrichment only (Zoominfo + multi-source + LLC piercing)."""
    from pipeline.orchestrator import run_enrichment_only
    asyncio.run(run_enrichment_only())


@cli.command("stats")
def stats():
    """Print current database stats."""
    from pipeline.orchestrator import print_stats
    asyncio.run(print_stats())


@cli.command("pierce")
@click.option("--entity", default=None, help="Entity name to pierce (exact match)")
@click.option("--address", default=None, help='Full address to research end-to-end, e.g. "123 Broadway, Brooklyn, NY 11201"')
@click.option("--skip-enrichment", is_flag=True, help="Skip contact enrichment (faster, no paid API spend)")
def pierce(entity: str, address: str, skip_enrichment: bool):
    """Pierce a specific LLC by name, or research a full address end-to-end."""
    if not entity and not address:
        raise click.UsageError("Provide either --entity or --address")
    if entity and address:
        raise click.UsageError("Provide only one of --entity or --address, not both")

    if address:
        async def _research():
            from pipeline.single_address import research_address
            click.echo(f"Researching: {address}")
            result = await research_address(address=address, skip_enrichment=skip_enrichment)
            if result.error:
                click.echo(f"Error: {result.error}")
                return
            cached_tag = " (cached)" if result.cached else ""
            click.echo(f"\nAddress : {result.address}{cached_tag}")
            click.echo(f"BBL     : {result.bbl}")
            click.echo(f"Prop ID : {result.property_id}")
            if result.hpd_reg_id:
                click.echo(f"HPD Reg : {result.hpd_reg_id}")
            _print_property_results(result.property_id)

        asyncio.run(_research())
    else:
        async def _pierce():
            from database.client import db, find_entity_by_name
            from enrichment.llc_piercer import pierce_entity

            ent = find_entity_by_name(entity)
            if not ent:
                click.echo(f"Entity not found: {entity}")
                click.echo("Creating entity and attempting to pierce...")
                from database.client import upsert_entity
                ent_id = upsert_entity(entity, "llc")
                ent = db().table("entities").select("*").eq("id", ent_id).execute().data[0]

            click.echo(f"Piercing: {ent['name']} (id={ent['id']})")
            success = await pierce_entity(ent)
            if success:
                click.echo("✓ Pierce successful — check entity_relationships table for results")
            else:
                click.echo("✗ Could not determine ownership through any strategy")

        asyncio.run(_pierce())


def _print_property_results(property_id: str) -> None:
    """Print owners, relationships, and contacts for a property."""
    if not property_id:
        return
    from database.client import db

    roles = (
        db().table("property_roles")
        .select("role, source, entities(*)")
        .eq("property_id", property_id)
        .execute().data or []
    )

    owners = [r for r in roles if r.get("role") == "owner" and r.get("entities")]
    managers = [r for r in roles if r.get("role") == "manager" and r.get("entities")]

    if not owners and not managers:
        click.echo("\nNo owner/manager entities found.")
        return

    click.echo("")
    for r in owners:
        e = r["entities"]
        pierced = "pierced" if e.get("is_pierced") else "not pierced"
        click.echo(f"  Owner [{r['source']}]: {e['name']} ({e['entity_type']}, {pierced})")

        rels = (
            db().table("entity_relationships")
            .select("confidence, source, entities!entity_relationships_parent_entity_id_fkey(name, entity_type)")
            .eq("child_entity_id", e["id"])
            .execute().data or []
        )
        for rel in rels:
            parent = rel.get("entities", {})
            click.echo(f"    -> Real owner [{rel.get('source')}, {rel.get('confidence', 0):.0%}]: "
                       f"{parent.get('name')} ({parent.get('entity_type')})")

        contacts = (
            db().table("contacts")
            .select("full_name, title, email, phone, source, network_role")
            .eq("entity_id", e["id"])
            .execute().data or []
        )
        for c in contacts:
            nr = f"[{c['network_role']}] " if c.get("network_role") else ""
            parts = [c.get("full_name", ""), c.get("title", ""), c.get("email", ""), c.get("phone", "")]
            click.echo(f"    Contact {nr}{' | '.join(p for p in parts if p)} [{c.get('source')}]")

    for r in managers:
        e = r["entities"]
        click.echo(f"  Manager [{r['source']}]: {e['name']}")


@cli.command("enrich-contacts")
@click.option("--signer-id", help="Contact UUID of a single mortgage signer to enrich")
@click.option("--limit", default=20, type=int, help="Batch size for pending signers")
@click.option("--force", is_flag=True, help="Re-run even if cached within STALE_DAYS")
def enrich_contacts(signer_id: str, limit: int, force: bool):
    """Run 3-prong contact enrichment on mortgage signers."""
    from enrichment.contact import enrich_signer, enrich_pending_signers
    if signer_id:
        stats = enrich_signer(signer_id, force=force)
        click.echo(str(stats))
    else:
        stats = enrich_pending_signers(limit=limit)
        click.echo(f"Processed {stats['processed']} signers — "
                   f"{stats['contacts_added']} contacts, {stats['companies_added']} companies added")


@cli.command("schedule")
def schedule():
    """Start the persistent background scheduler (for production use)."""
    import scheduler as sched
    sched.main()


@cli.command("ingest")
@click.argument("source", type=click.Choice(["hpd", "acris", "wow"]))
def ingest(source: str):
    """Run a single ingestor. (OpenCorporates removed — price prohibitive.)"""
    async def _run():
        if source == "hpd":
            from ingest.hpd import run
            await run()
        elif source == "acris":
            from ingest.acris import run
            await run()
        elif source == "wow":
            from ingest.whoownswhat import run
            await run()

    asyncio.run(_run())
    click.echo(f"Ingestor '{source}' complete.")


if __name__ == "__main__":
    cli()
