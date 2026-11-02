import { DOMAINS_FILTER_NAME } from '@app/searchV2/utils/constants';
import { prependMissingUrnEntities } from '@app/sharedV2/utils/mergeUrnEntities';

import { Document, Domain, Entity, EntityType, FacetMetadata } from '@types';

/** Documents sidebar grouping option values (display-select pills). */
export const DOCUMENT_GROUP_BY = {
    SOURCE: 'source',
    DOMAIN: 'domain',
} as const;

export type DocumentGroupByValue = (typeof DOCUMENT_GROUP_BY)[keyof typeof DOCUMENT_GROUP_BY];

export const UNASSIGNED_DOMAIN_GROUP_KEY = '__unassigned_domain__';

export function isDocumentGroupByValue(value: string): value is DocumentGroupByValue {
    return value === DOCUMENT_GROUP_BY.SOURCE || value === DOCUMENT_GROUP_BY.DOMAIN;
}

export type DocumentDomainGroup = {
    key: string;
    label: string;
    entity?: Domain;
};

type BuildDocumentDomainGroupsOptions = {
    aggregations: FacetMetadata['aggregations'];
    unassignedCount: number;
    unassignedLabel: string;
    activeGroup?: DocumentDomainGroup;
    getDisplayName: (entity: Domain) => string;
};

function isDomain(entity: Entity | null | undefined): entity is Domain {
    return entity?.type === EntityType.Domain;
}

/**
 * Maps domain facet aggregations into sidebar group headers, appending Unassigned
 * and the active document's domain when the facet page omitted them.
 */
export function buildDocumentDomainGroups({
    aggregations,
    unassignedCount,
    unassignedLabel,
    activeGroup,
    getDisplayName,
}: BuildDocumentDomainGroupsOptions): DocumentDomainGroup[] {
    const groups = aggregations
        .filter((aggregation) => aggregation.count > 0 && !!aggregation.value)
        .map<DocumentDomainGroup>((aggregation) => {
            const entity = isDomain(aggregation.entity) ? aggregation.entity : undefined;
            return {
                key: aggregation.value,
                label: entity ? getDisplayName(entity) : aggregation.value,
                entity,
            };
        });

    if (unassignedCount > 0 || activeGroup?.key === UNASSIGNED_DOMAIN_GROUP_KEY) {
        groups.push({
            key: UNASSIGNED_DOMAIN_GROUP_KEY,
            label: unassignedLabel,
        });
    }

    if (activeGroup && !groups.some((group) => group.key === activeGroup.key)) {
        groups.push(activeGroup);
    }

    return groups.sort((left, right) => left.label.localeCompare(right.label));
}

type DocumentGroupingEntityData = {
    urn: string;
    domain?: Domain | null;
};

/** Resolve which domain group owns the open document so that section can auto-expand. */
export function resolveActiveDocumentDomainGroup(
    entityData: DocumentGroupingEntityData | null,
    selectedUrn: string | null,
    unassignedLabel: string,
    getDisplayName: (entity: Domain) => string,
): DocumentDomainGroup | undefined {
    if (!entityData || entityData.urn !== selectedUrn) return undefined;

    if (entityData.domain) {
        return {
            key: entityData.domain.urn,
            label: getDisplayName(entityData.domain),
            entity: entityData.domain,
        };
    }

    return {
        key: UNASSIGNED_DOMAIN_GROUP_KEY,
        label: unassignedLabel,
    };
}

export function isUnassignedDocumentDomainGroup(groupKey: string): boolean {
    return groupKey === UNASSIGNED_DOMAIN_GROUP_KEY;
}

/** Facet field used when aggregating document domain groups. */
export function getDocumentDomainGroupField(): string {
    return DOMAINS_FILTER_NAME;
}

/** searchDocuments filters for one domain group header (including Unassigned). */
export function buildDocumentDomainGroupSearchInput(groupKey: string): {
    domains?: string[];
    hasDomain?: boolean;
} {
    if (isUnassignedDocumentDomainGroup(groupKey)) {
        return { hasDomain: false };
    }
    return { domains: [groupKey] };
}

type DomainGroupDocument = { urn: string; domain?: Document['domain'] };

/** True when the document's domain association matches the group header key. */
export function documentBelongsInDomainGroup(
    document: Pick<Document, 'domain'> | null | undefined,
    groupKey: string,
): boolean {
    if (!document) return false;
    const domainUrn = document.domain?.domain?.urn;
    if (isUnassignedDocumentDomainGroup(groupKey)) return !domainUrn;
    return domainUrn === groupKey;
}

/**
 * Drop the open document from a group it has left, and prepend it onto the group
 * it now belongs to when that group's loaded page doesn't include it yet.
 */
export function documentsInDomainGroup<T extends DomainGroupDocument>(
    documents: T[],
    groupKey: string,
    selectedDocument?: T | null,
): T[] {
    if (!selectedDocument) return documents;

    const belongs = documentBelongsInDomainGroup(selectedDocument, groupKey);
    const visible = belongs ? documents : documents.filter((document) => document.urn !== selectedDocument.urn);
    if (!belongs) return visible;
    return prependMissingUrnEntities(visible, [selectedDocument]);
}

/**
 * Refetch domain group pages only when the same open document moves between groups.
 * Opening a different document in another group must not remount the tree.
 * First resolution and navigate-away must not refetch.
 */
export function shouldRefetchDocumentDomainGroups(
    previousUrn: string | null | undefined,
    previousGroupKey: string | null | undefined,
    nextUrn: string | null | undefined,
    nextGroupKey: string | null | undefined,
): boolean {
    return (
        !!previousUrn &&
        previousUrn === nextUrn &&
        !!previousGroupKey &&
        !!nextGroupKey &&
        previousGroupKey !== nextGroupKey
    );
}

export function getDocumentSidebarTitle(document: Document, untitledFallback: string): string {
    return document.info?.title || untitledFallback;
}

/** Same separator as move-popover / document search breadcrumb rows. */
export const DOCUMENT_PARENT_BREADCRUMB_SEPARATOR = ' > ';

/**
 * Root-first parent path for flat sidebar rows (domain groups, search hits).
 * `parentDocuments` is ordered nearest-parent first from searchDocuments.
 */
export function buildDocumentParentBreadcrumb(
    document: Pick<Document, 'parentDocuments'> | null | undefined,
    untitledFallback: string,
    separator: string = DOCUMENT_PARENT_BREADCRUMB_SEPARATOR,
): string | null {
    const parents = document?.parentDocuments?.documents;
    if (!parents?.length) return null;
    return [...parents]
        .reverse()
        .map((parent) => parent?.info?.title || untitledFallback)
        .join(separator);
}

/** Minimal shape for {@link resolveActiveDocumentDomainGroup} from a loaded Document. */
export function toDocumentGroupingEntityData(document: Document | null): DocumentGroupingEntityData | null {
    if (!document) return null;
    return {
        urn: document.urn,
        domain: document.domain?.domain ?? null,
    };
}
