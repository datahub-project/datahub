import { CorpUser, DataHubPolicy, DataHubRole } from '@types';

/**
 * Filters a role relationship list down to the edges whose entity actually
 * resolved, warning once when any are dropped.
 *
 * A role's relationship lists (`policies`, `users`) are built from graph edges
 * whose source/target type can be heterogeneous. For example, a role's
 * `policies` come from incoming `IsAssociatedWithRole` edges, but that
 * relationship name is shared — it is emitted toward a role by both policy actor
 * filters and the global-settings maintenance-window audience — so a non-policy
 * source resolves to `entity: null` under the `... on DataHubPolicy` fragment.
 * Rendering such a null (reading `.urn` for a React key) crashes the Roles page.
 *
 * The `listRoles` query already constrains the policies edge server-side via
 * `relatedEntityTypes: ["dataHubPolicy"]`, so nulls should no longer reach the
 * client. This filter is a defensive backstop: a fired warning signals an
 * unexpected unresolved edge that slipped past the backend.
 */
function getResolvedRelationshipEntities<T>(
    role: DataHubRole | null | undefined,
    relationships: ReadonlyArray<{ entity?: T | null } | null> | null | undefined,
    entityKind: string,
): T[] {
    const entities = (relationships ?? []).map((relationship) => relationship?.entity);
    const resolved = entities.filter((entity): entity is T => entity != null);

    const droppedCount = entities.length - resolved.length;
    if (droppedCount > 0) {
        // eslint-disable-next-line no-console
        console.warn(
            `Role "${role?.urn}" returned ${droppedCount} relationship(s) that did not resolve to a ` +
                `${entityKind} and were dropped. The backend should already exclude such edges, so this ` +
                `indicates an unexpected entity associated with the role.`,
        );
    }

    return resolved;
}

/**
 * Extracts the resolved DataHubPolicy entities associated with a role, dropping
 * any unresolved (null) edges. See {@link getResolvedRelationshipEntities}.
 */
export function getRolePolicies(role: DataHubRole | null | undefined): DataHubPolicy[] {
    return getResolvedRelationshipEntities<DataHubPolicy>(
        role,
        (role as any)?.policies?.relationships,
        'DataHubPolicy',
    );
}

/**
 * Extracts the resolved member entities (users/groups) associated with a role,
 * dropping any unresolved (null) edges. See {@link getResolvedRelationshipEntities}.
 */
export function getRoleUsers(role: DataHubRole | null | undefined): CorpUser[] {
    return getResolvedRelationshipEntities<CorpUser>(role, (role as any)?.users?.relationships, 'CorpUser');
}
