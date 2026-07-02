#!/usr/bin/env python3
"""One-time migration: rewrite legacy Material UI icon names on Domain displayProperties.

The DataHub UI moved its domain icon library from `@mui/icons-material` to
`@phosphor-icons/react`. Domains created before that switch persist MUI names
(e.g. `AccountCircle`, `Business`) in their `displayProperties.icon.name` field.
This script iterates every Domain, translates any MUI name to its Phosphor
equivalent using the table below, and writes the updated aspect back — after
which the frontend never has to do a render-time translation.

Idempotent — running it a second time is a no-op: entries already rewritten
are no longer in the MUI table so they pass through unchanged. Domains whose
icon name is unknown (neither MUI nor Phosphor) are left alone so the frontend
falls back to its letter avatar rather than the raw `AppWindow` fallback.

Usage:
    # Dry run — prints what would change, writes nothing.
    python domain_migrate_mui_icons_to_phosphor.py --dry-run

    # Real run against a specific instance.
    DATAHUB_GMS_URL=https://datahub.mycorp.com \\
    DATAHUB_GMS_TOKEN=... \\
        python domain_migrate_mui_icons_to_phosphor.py

    # Or pass connection args explicitly.
    python domain_migrate_mui_icons_to_phosphor.py \\
        --gms-url http://localhost:8080 --token <PAT>
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Dict

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    DisplayPropertiesClass,
    IconLibraryClass,
    IconPropertiesClass,
)

# Kept in lockstep with the historical frontend shim at
# datahub-web-react/src/app/sharedV2/icons/muiToPhosphor.ts before it was removed.
# The pre-migration default was `AccountCircle`; the rest of the table covers the
# icons most likely to have been picked for business-domain avatars.
MUI_TO_PHOSPHOR: Dict[str, str] = {
    # Pre-migration default — must map or nearly every legacy domain regresses
    # to the letter-avatar fallback.
    "AccountCircle": "UserCircle",
    # People / users
    "Person": "User",
    "People": "Users",
    "Group": "Users",
    "Groups": "Users",
    "SupervisorAccount": "UserGear",
    # Places / structure
    "Home": "House",
    "Business": "Buildings",
    "Domain": "Buildings",
    "Store": "Storefront",
    "Storefront": "Storefront",
    "Warehouse": "Warehouse",
    "Factory": "Factory",
    "LocationOn": "MapPin",
    "LocationCity": "Buildings",
    "Place": "MapPin",
    "Map": "MapPin",
    "Public": "Globe",
    "Language": "Globe",
    "Explore": "Compass",
    # Data / storage
    "Storage": "Database",
    "Dns": "HardDrives",
    "Cloud": "Cloud",
    "Folder": "Folder",
    "Description": "FileText",
    "Article": "Article",
    "Assignment": "ClipboardText",
    "LibraryBooks": "Books",
    "Book": "Book",
    "MenuBook": "BookOpen",
    # Analytics / charts
    "BarChart": "ChartBar",
    "ShowChart": "ChartLine",
    "PieChart": "ChartPie",
    "DonutLarge": "ChartDonut",
    "BubbleChart": "ChartDonut",
    "Analytics": "ChartLineUp",
    "Assessment": "ChartLineUp",
    "Leaderboard": "ChartBar",
    "TrendingUp": "TrendUp",
    "TrendingDown": "TrendDown",
    "Insights": "Lightbulb",
    "Speed": "Gauge",
    "Timeline": "ChartLine",
    # Development / tech
    "Code": "Code",
    "Api": "Plug",
    "Terminal": "Terminal",
    "Build": "Wrench",
    "Engineering": "Wrench",
    "Devices": "Monitor",
    "Laptop": "Laptop",
    "PhoneAndroid": "DeviceMobile",
    "Router": "WifiHigh",
    "BugReport": "Bug",
    "Memory": "Cpu",
    "DeveloperMode": "Code",
    # UI containers
    "Dashboard": "SquaresFour",
    "Category": "SquaresFour",
    "Widgets": "SquaresFour",
    "ViewModule": "SquaresFour",
    "Layers": "Stack",
    "Extension": "PuzzlePiece",
    # Commerce / money
    "ShoppingCart": "ShoppingCart",
    "ShoppingCartCheckout": "ShoppingCart",
    "ShoppingBag": "ShoppingBag",
    "ShoppingBasket": "Basket",
    "LocalMall": "ShoppingBag",
    "LocalGroceryStore": "ShoppingCart",
    "LocalShipping": "Truck",
    "LocalOffer": "Tag",
    "Sell": "Tag",
    "AttachMoney": "CurrencyDollar",
    "Payments": "Money",
    "Receipt": "Receipt",
    "CreditCard": "CreditCard",
    "MonetizationOn": "Coin",
    "AccountBalance": "Bank",
    "AccountBalanceWallet": "Wallet",
    "Savings": "PiggyBank",
    "Redeem": "Gift",
    "CardGiftcard": "Gift",
    "Campaign": "Megaphone",
    "Loyalty": "Heart",
    "Handshake": "Handshake",
    # Communication
    "Email": "Envelope",
    "Mail": "Envelope",
    "Chat": "ChatCircle",
    "Message": "ChatCircleText",
    "Notifications": "Bell",
    "Phone": "Phone",
    # Security
    "Security": "Shield",
    "Lock": "Lock",
    "VpnKey": "Key",
    "Fingerprint": "Fingerprint",
    # Status / feedback
    "Info": "Info",
    "Warning": "Warning",
    "Error": "WarningCircle",
    "CheckCircle": "CheckCircle",
    "Star": "Star",
    "Favorite": "Heart",
    "Flag": "Flag",
    "Bookmark": "Bookmark",
    # Time
    "Schedule": "Clock",
    "Event": "Calendar",
    # Misc common
    "Work": "Briefcase",
    "WorkOutline": "Briefcase",
    "School": "GraduationCap",
    "Settings": "Gear",
    "Search": "MagnifyingGlass",
    "Palette": "Palette",
    "Rocket": "Rocket",
    "Edit": "Pencil",
    "Create": "Pencil",
    "Note": "Note",
    "Notes": "Notebook",
    "Camera": "Camera",
    "Videocam": "VideoCamera",
    "PlayArrow": "Play",
    "Send": "PaperPlaneTilt",
    "Whatshot": "Fire",
    "EmojiEvents": "Trophy",
    "LocalHospital": "FirstAid",
    "Restaurant": "ForkKnife",
    "LocalCafe": "Coffee",
    "FlightTakeoff": "Airplane",
    "DirectionsBoat": "Boat",
    "DirectionsCar": "Car",
    "Traffic": "TrafficSign",
    "Brush": "PaintBrush",
    "ContentCut": "Scissors",
    "Handyman": "Toolbox",
    "Construction": "Hammer",
    "StraightenOutlined": "Ruler",
    "Podcasts": "Broadcast",
    "LiveTv": "MonitorPlay",
}

logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--gms-url",
        default=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
        help="DataHub GMS URL. Defaults to $DATAHUB_GMS_URL or http://localhost:8080.",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("DATAHUB_GMS_TOKEN"),
        help="Personal access token. Defaults to $DATAHUB_GMS_TOKEN.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would change without emitting any updates.",
    )
    return parser.parse_args()


def _rewrite_icon(graph: DataHubGraph, urn: str, dry_run: bool) -> str:
    """Rewrite a single domain's icon if it's a legacy MUI name.

    Returns one of: "rewrote", "already_phosphor", "no_icon", "unknown".
    """
    props = graph.get_aspect(entity_urn=urn, aspect_type=DisplayPropertiesClass)
    if props is None or props.icon is None or not props.icon.name:
        return "no_icon"

    current_name = props.icon.name
    phosphor_name = MUI_TO_PHOSPHOR.get(current_name)
    if phosphor_name is None:
        # Either already a Phosphor name (post-migration write) or an unmapped
        # legacy value — either way, leave it alone.
        return "already_phosphor"
    if phosphor_name == current_name:
        # Identity mapping (e.g. Storefront -> Storefront). No-op write skipped.
        return "already_phosphor"

    updated = DisplayPropertiesClass(
        colorHex=props.colorHex,
        # Keep iconLibrary=MATERIAL and the stored `style` unchanged — those
        # fields are historical constants the GMS resolver still expects.
        # See datahub-web-react/src/app/entityV2/domain/utils/displayProperties.ts.
        icon=IconPropertiesClass(
            iconLibrary=IconLibraryClass.MATERIAL,
            name=phosphor_name,
            style=props.icon.style or "Outlined",
        ),
    )
    if dry_run:
        logger.info("[dry-run] %s: %s -> %s", urn, current_name, phosphor_name)
    else:
        graph.emit_mcp(MetadataChangeProposalWrapper(entityUrn=urn, aspect=updated))
        logger.info("%s: %s -> %s", urn, current_name, phosphor_name)
    return "rewrote"


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    args = _parse_args()

    graph = DataHubGraph(DatahubClientConfig(server=args.gms_url, token=args.token))

    counts = {"rewrote": 0, "already_phosphor": 0, "no_icon": 0, "unknown": 0}
    for urn in graph.get_urns_by_filter(entity_types=["domain"], query="*"):
        try:
            counts[_rewrite_icon(graph, urn, args.dry_run)] += 1
        except Exception:
            # Log and continue — one bad domain shouldn't stall the migration.
            logger.exception("Failed to migrate %s; skipping", urn)
            counts["unknown"] += 1

    total = sum(counts.values())
    logger.info(
        "Done. scanned=%d rewrote=%d already_phosphor=%d no_icon=%d failed=%d%s",
        total,
        counts["rewrote"],
        counts["already_phosphor"],
        counts["no_icon"],
        counts["unknown"],
        " (dry run — no writes emitted)" if args.dry_run else "",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
