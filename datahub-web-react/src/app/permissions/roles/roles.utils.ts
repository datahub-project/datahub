import { DataHubPolicy, DataHubRole } from '@types';

/**
 * Extracts the DataHubPolicy entities associated with a role.
 *
 * A role's `policies` list comes from incoming `IsAssociatedWithRole` graph
 * edges. That relationship name is shared — it is emitted toward a role by both
 * policy actor filters and the global-settings maintenance-window audience — so
 * the raw edge set can contain non-policy sources that resolve to `entity: null`
 * under the `... on DataHubPolicy` fragment.
 *
 * The `listRoles` query constrains this server-side via
 * `relatedEntityTypes: ["dataHubPolicy"]`, so nulls should no longer reach the
 * client. This filter is a defensive backstop: rendering a null entity (reading
 * `.urn` for a React key) crashes the Roles page, so we drop any nulls and warn.
 * A fired warning now signals an unexpected non-policy edge that slipped past
 * the backend filter.
 */
export function getRolePolicies(role: DataHubRole | null | undefined): DataHubPolicy[] {
    const entities = (role as any)?.policies?.relationships?.map((relationship) => relationship?.entity) ?? [];
    const policies = entities.filter((entity): entity is DataHubPolicy => entity != null);

    const droppedCount = entities.length - policies.length;
    if (droppedCount > 0) {
        // eslint-disable-next-line no-console
        console.warn(
            `Role "${role?.urn}" returned ${droppedCount} policy relationship(s) that did not resolve to a ` +
                `DataHubPolicy and were dropped. The backend should already exclude non-policy edges, so this ` +
                `indicates an unexpected non-policy entity associated with the role.`,
        );
    }

    return policies;
}
