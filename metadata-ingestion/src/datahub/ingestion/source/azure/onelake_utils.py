def strip_onelake_prefix(path: str) -> str:
    """Strip the OneLake prefix from an ABFSS path."""
    if path.startswith("abfss://"):
        # Split on @ and take everything after the first slash after onelake.dfs.fabric.microsoft.com
        parts = path.split("@")[1].split("/", 1)
        if len(parts) > 1:
            return parts[1]
    return path
