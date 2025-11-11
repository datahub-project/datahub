/**
 * Compare versions for exact match
 * Returns true if currentVersion === requiredVersion (normalized)
 *
 * Examples:
 *   isVersionMatch("v1.4.0", "v1.4.0") => true
 *   isVersionMatch("1.4.0", "v1.4.0") => true
 *   isVersionMatch("v1.3.10", "v1.3.15") => false
 *   isVersionMatch("v1.4.0", null) => true (no required version = show to all)
 *   isVersionMatch(null, "v1.4.0") => false (no current version = can't verify)
 */
export function isVersionMatch(
    currentVersion: string | null | undefined,
    requiredVersion: string | null | undefined,
): boolean {
    // If no required version specified (including string "null" or JSON null), show update to all versions
    if (!requiredVersion || requiredVersion === 'null' || requiredVersion === null) return true;

    // If no current version, don't show (can't verify compatibility)
    if (!currentVersion) return false;

    // Normalize versions (remove whitespace first, then 'v' prefix)
    const normalize = (version: string) => version.trim().replace(/^v/, '');

    const current = normalize(currentVersion);
    const required = normalize(requiredVersion);

    // If normalized current version is empty, can't verify
    if (!current) return false;

    // Exact match comparison
    return current === required;
}
