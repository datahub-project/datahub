/** Drops domains whose ancestor is also selected, since "include subdomains" already covers them. */
export function withoutCoveredSubdomains(urns: string[], ancestorsByUrn: Record<string, string[]>): string[] {
    const selected = new Set(urns);
    return urns.filter((urn) => !(ancestorsByUrn[urn] ?? []).some((ancestor) => selected.has(ancestor)));
}
