#!/usr/bin/env python3
"""
Modular CLI for DataHub RDF operations using dependency injection.

This script provides a clean interface for processing RDF files and converting
them to DataHub entities using the modular orchestrator architecture.
"""

import argparse
import logging
import os
from pathlib import Path

from datahub.ingestion.source.rdf.core import (
    DataHubClient,
    Orchestrator,
    QueryFactory,
    RDFToDataHubTranspiler,
    SourceFactory,
    TargetFactory,
)
from datahub.ingestion.source.rdf.dialects import RDFDialect
from datahub.ingestion.source.rdf.entities.registry import create_default_registry

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def resolve_datahub_config(args):
    """
    Resolve DataHub server and token from CLI arguments and environment variables.

    Priority order:
    1. CLI arguments (--server, --token)
    2. Environment variables (DATAHUB_SERVER, DATAHUB_TOKEN)
    3. Error if neither CLI nor env vars provide both server and token

    Returns:
        tuple: (server, token) or raises ValueError if not found
    """
    # Get from CLI args first
    server = args.datahub_server
    token = args.datahub_token

    # Fall back to environment variables if CLI args not provided
    if server is None:
        server = os.getenv("DATAHUB_SERVER")
    if token is None:
        token = os.getenv("DATAHUB_TOKEN")

    # Check if we have server (token can be None or empty string for unauthenticated access)
    if not server or server.strip() == "":
        raise ValueError(
            "DataHub server required. Provide via:\n"
            "  CLI: --datahub-server <url> [--datahub-token <token>]\n"
            "  Environment: DATAHUB_SERVER=<url> [DATAHUB_TOKEN=<token>]\n"
            "  Or use --dry-run for pretty print output"
        )

    # Empty tokens are allowed for unauthenticated access
    # Only reject if token is explicitly set to None when it shouldn't be

    return server, token


def create_source_from_args(args):
    """Create source based on command line arguments."""
    if not args.source:
        raise ValueError(
            "No source specified. Use --source with a file, folder, or server URL"
        )

    source_path = args.source

    # Check if it's a server URL
    if source_path.startswith(("http://", "https://")):
        return SourceFactory.create_server_source(source_path, args.format)

    # Check if it's a folder
    path = Path(source_path)
    if path.is_dir():
        return SourceFactory.create_folder_source(
            source_path,
            recursive=not args.no_recursive,
            file_extensions=args.extensions,
        )

    # Check if it's a single file
    if path.is_file():
        return SourceFactory.create_file_source(source_path, args.format)

    # Check if it's a glob pattern or multiple files (comma-separated)
    if "," in source_path:
        files = [f.strip() for f in source_path.split(",")]
        return SourceFactory.create_multi_file_source(files, args.format)

    # Try to find files matching the pattern
    import glob

    matching_files = glob.glob(source_path)
    if matching_files:
        if len(matching_files) == 1:
            return SourceFactory.create_file_source(matching_files[0], args.format)
        else:
            return SourceFactory.create_multi_file_source(matching_files, args.format)

    raise ValueError(f"Source not found: {source_path}")


def create_query_from_args(args):
    """Create query based on command line arguments."""
    if args.sparql:
        return QueryFactory.create_sparql_query(args.sparql, "Custom SPARQL Query")
    elif args.filter:
        # Parse filter criteria
        filter_criteria = {}
        for filter_arg in args.filter:
            if "=" in filter_arg:
                key, value = filter_arg.split("=", 1)
                filter_criteria[key] = value
        return QueryFactory.create_filter_query(filter_criteria, "Filter Query")
    else:
        # Default to pass-through query
        return QueryFactory.create_pass_through_query("Pass-through Query")


def create_target_from_args(args):
    """Create target based on command line arguments."""
    if args.ownership_output:
        # Ownership export mode
        format_type = args.ownership_format or "json"
        return TargetFactory.create_ownership_export_target(
            args.ownership_output, format_type
        )
    elif args.ddl_output:
        # DDL export mode - not supported in MVP
        raise ValueError(
            "DDL export is not supported in MVP. Dataset export has been removed."
        )
    elif args.output_file:
        return TargetFactory.create_file_target(args.output_file, args.output_format)
    elif args.dry_run:
        # Explicit dry run mode
        # PrettyPrintTarget can work without a URN generator (it's optional)
        return TargetFactory.create_pretty_print_target()
    else:
        # Default to live mode - resolve server and token from CLI args or env vars
        try:
            server, token = resolve_datahub_config(args)
            datahub_client = DataHubClient(server, token)

            return TargetFactory.create_datahub_target(datahub_client)
        except ValueError as e:
            # If no server/token found, provide helpful error message
            raise ValueError(f"Live mode requires DataHub configuration: {e}") from e


def create_transpiler_from_args(args):
    """Create transpiler based on command line arguments."""
    # Environment is defaulted at CLI entry point, then passed through
    environment = args.environment

    # Parse dialect if provided
    forced_dialect = None
    if args.dialect:
        forced_dialect = RDFDialect(args.dialect)

    # Parse filtering parameters
    export_only = (
        args.export_only if hasattr(args, "export_only") and args.export_only else None
    )
    skip_export = (
        args.skip_export if hasattr(args, "skip_export") and args.skip_export else None
    )

    return RDFToDataHubTranspiler(
        environment,
        forced_dialect=forced_dialect,
        export_only=export_only,
        skip_export=skip_export,
    )


def main():
    """Main CLI function with dependency injection."""
    parser = argparse.ArgumentParser(
        description="Modular DataHub RDF processor using dependency injection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process single file with live DataHub (default mode)
  python -m rdf.scripts.datahub_rdf --source data.ttl --datahub-server http://localhost:8080 --datahub-token your_token

  # Process folder recursively with environment variables
  DATAHUB_SERVER=http://localhost:8080 DATAHUB_TOKEN=your_token python -m rdf.scripts.datahub_rdf --source ./data

  # Process multiple files (comma-separated)
  python -m rdf.scripts.datahub_rdf --source file1.ttl,file2.ttl,file3.ttl --dry-run

  # Process with pretty print output (dry run)
  python -m rdf.scripts.datahub_rdf --source data.ttl --dry-run

  # Export datasets as DDL (auto-detect dialect from platforms)
  python -m rdf.scripts.datahub_rdf --source data.ttl --ddl-output schema.sql

  # Export datasets as DDL (force specific dialect)
  python -m rdf.scripts.datahub_rdf --source data.ttl --ddl-output schema.sql --ddl-dialect mysql

  # Export ownership information
  python -m rdf.scripts.datahub_rdf --source data.ttl --ownership-output ownership.json --ownership-format json

  # Process with SPARQL query and file output
  python -m rdf.scripts.datahub_rdf --source data.ttl --sparql "SELECT * WHERE { ?s ?p ?o }" --output-file results.json

  # Process with filter and custom extensions
  python -m rdf.scripts.datahub_rdf --source ./data --filter "namespace=http://example.com/" --extensions .ttl .rdf

  # Process remote server
  python -m rdf.scripts.datahub_rdf --source http://example.com/sparql --dry-run
        """,
    )

    # Source arguments
    source_group = parser.add_argument_group("Source Options")
    source_group.add_argument(
        "--source",
        required=True,
        help="Source to process: file path, folder path, server URL, or comma-separated files",
    )
    source_group.add_argument(
        "--format", help="RDF format (auto-detected if not specified)"
    )
    source_group.add_argument(
        "--extensions",
        nargs="+",
        default=[".ttl", ".rdf", ".owl", ".n3", ".nt"],
        help="File extensions to process (default: .ttl .rdf .owl .n3 .nt)",
    )
    source_group.add_argument(
        "--no-recursive",
        action="store_true",
        help="Disable recursive folder processing",
    )

    # Query arguments
    query_group = parser.add_argument_group("Query Options")
    query_group.add_argument("--sparql", help="SPARQL query to execute")
    query_group.add_argument("--filter", nargs="+", help="Filter criteria (key=value)")

    # Target arguments
    target_group = parser.add_argument_group("Target Options")
    target_group.add_argument(
        "--dry-run",
        action="store_true",
        help="Pretty print output instead of sending to DataHub (default: live mode)",
    )
    target_group.add_argument("--output-file", help="Output file path")
    target_group.add_argument("--output-format", help="Output format (required)")

    # DDL export arguments
    ddl_group = parser.add_argument_group("DDL Export Options")
    ddl_group.add_argument(
        "--ddl-output", help="Export datasets as DDL to specified file"
    )
    ddl_group.add_argument(
        "--ddl-dialect",
        choices=["postgresql", "mysql", "sqlite", "sqlserver", "oracle"],
        help="SQL dialect for DDL export (auto-detected from dataset platforms if not specified)",
    )

    # Ownership export arguments
    ownership_group = parser.add_argument_group("Ownership Export Options")
    ownership_group.add_argument(
        "--ownership-output", help="Export ownership information to specified file"
    )
    ownership_group.add_argument(
        "--ownership-format",
        choices=["json", "csv", "yaml"],
        default="json",
        help="Format for ownership export (default: json)",
    )

    # DataHub arguments
    datahub_group = parser.add_argument_group("DataHub Options")
    datahub_group.add_argument(
        "--datahub-server", help="DataHub GMS URL (or set DATAHUB_SERVER env var)"
    )
    datahub_group.add_argument(
        "--datahub-token",
        nargs="?",
        help="DataHub API token (or set DATAHUB_TOKEN env var)",
    )
    datahub_group.add_argument(
        "--environment", default="PROD", help="DataHub environment (default: PROD)"
    )

    # Selective export arguments
    # Get CLI choices from registry (ownership is a special export target, not an entity type)
    registry = create_default_registry()
    cli_choices = registry.get_all_cli_choices()
    # Add 'ownership' as a special export target (not an entity type)
    if "ownership" not in cli_choices:
        cli_choices.append("ownership")
    cli_choices = sorted(cli_choices)

    export_group = parser.add_argument_group("Selective Export Options")
    export_group.add_argument(
        "--export-only",
        nargs="+",
        choices=cli_choices,
        help="Export only specified entity types to DataHub (e.g., --export-only data_products)",
    )
    export_group.add_argument(
        "--skip-export",
        nargs="+",
        choices=cli_choices,
        help="Skip exporting specified entity types to DataHub (e.g., --skip-export glossary datasets)",
    )

    # General arguments
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )
    parser.add_argument(
        "--validate-only", action="store_true", help="Only validate configuration"
    )
    parser.add_argument(
        "--dialect",
        choices=[d.value for d in RDFDialect],
        help="Force a specific RDF dialect (default: auto-detect)",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        logger.info("Starting modular DataHub RDF processor")

        # Create components using dependency injection
        logger.info("Creating components with dependency injection...")

        source = create_source_from_args(args)
        query = create_query_from_args(args)
        target = create_target_from_args(args)
        transpiler = create_transpiler_from_args(args)

        # Create orchestrator
        orchestrator = Orchestrator(source, query, target, transpiler)

        # Validate configuration
        logger.info("Validating pipeline configuration...")
        validation_results = orchestrator.validate()

        if not validation_results["valid"]:
            logger.error("❌ Pipeline configuration validation failed")
            print("Validation Errors:")
            for key, value in validation_results.items():
                if key.endswith("_error"):
                    print(f"  {key}: {value}")
            return 1

        logger.info("✅ Pipeline configuration validation passed")

        if args.validate_only:
            logger.info("Validation-only mode - configuration is valid")
            print("Pipeline Configuration:")
            pipeline_info = orchestrator.get_pipeline_info()
            for component, info in pipeline_info.items():
                print(f"  {component}: {info}")
            return 0

        # Execute pipeline
        logger.info("Executing pipeline...")
        results = orchestrator.execute()

        if results["success"]:
            logger.info("✅ Pipeline execution completed successfully")

            # Print target results
            target_results = results["target_results"]
            if target_results["target_type"] == "pretty_print":
                print(
                    target_results["results"].get(
                        "pretty_output", "No output available"
                    )
                )
            elif target_results["target_type"] == "datahub":
                print("\nDataHub Results:")
                print(f"  Success: {target_results['success']}")
            elif target_results["target_type"] == "file":
                print("\nFile Output:")
                print(f"  File: {target_results['output_file']}")
                print(f"  Success: {target_results['success']}")
            elif target_results["target_type"] == "ddl":
                print("\nDDL Export Results:")
                print(f"  Output File: {target_results['output_file']}")
                print(f"  Dialect: {target_results['dialect']}")
                print(
                    f"  Tables Created: {target_results['results'].get('tables_created', 0)}"
                )
                print(f"  Success: {target_results['success']}")
            elif target_results["target_type"] == "ownership_export":
                print("\nOwnership Export Results:")
                print(f"  Output File: {target_results['output_file']}")
                print(f"  Format: {target_results['format']}")
                print(f"  Ownership Records: {target_results['ownership_count']}")
                print(f"  Success: {target_results['success']}")

            return 0
        else:
            logger.error("❌ Pipeline execution failed")
            error_msg = results.get("error")
            if not error_msg:
                raise ValueError(
                    "Pipeline execution failed but no error message provided"
                )
            print(f"Error: {error_msg}")
            return 1

    except Exception as e:
        logger.error(f"CLI execution failed: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
