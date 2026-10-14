import {
    UNASSIGNED_DOMAIN_GROUP_KEY,
    buildDocumentDomainGroupSearchInput,
    buildDocumentDomainGroups,
    buildDocumentParentBreadcrumb,
    documentBelongsInDomainGroup,
    documentsInDomainGroup,
    getDocumentDomainGroupField,
    getDocumentSidebarTitle,
    isDocumentGroupByValue,
    isUnassignedDocumentDomainGroup,
    resolveActiveDocumentDomainGroup,
    shouldRefetchDocumentDomainGroups,
    toDocumentGroupingEntityData,
} from '@app/document/utils/documentSidebarGrouping';

import { Document, Domain, EntityType, FacetMetadata } from '@types';

const domain = {
    __typename: 'Domain',
    urn: 'urn:li:domain:marketing',
    type: EntityType.Domain,
} as Domain;

describe('document sidebar grouping', () => {
    it('recognizes grouping values', () => {
        expect(isDocumentGroupByValue('source')).toBe(true);
        expect(isDocumentGroupByValue('domain')).toBe(true);
        expect(isDocumentGroupByValue('platform')).toBe(false);
    });

    it('maps the domain group field to the indexed facet', () => {
        expect(getDocumentDomainGroupField()).toBe('domains');
        expect(isUnassignedDocumentDomainGroup(UNASSIGNED_DOMAIN_GROUP_KEY)).toBe(true);
        expect(isUnassignedDocumentDomainGroup(domain.urn)).toBe(false);
    });

    it('maps facet aggregations to sorted domain groups', () => {
        const aggregations = [
            { value: domain.urn, count: 2, entity: domain },
            { value: 'urn:li:domain:empty', count: 0 },
        ] as FacetMetadata['aggregations'];

        expect(
            buildDocumentDomainGroups({
                aggregations,
                unassignedCount: 0,
                unassignedLabel: 'Unassigned',
                getDisplayName: () => 'Marketing',
            }),
        ).toEqual([
            {
                key: domain.urn,
                label: 'Marketing',
                entity: domain,
            },
        ]);
    });

    it('adds unassigned and active domain groups when facets omit them', () => {
        const activeGroup = {
            key: domain.urn,
            label: 'Marketing',
            entity: domain,
        };

        expect(
            buildDocumentDomainGroups({
                aggregations: [],
                unassignedCount: 3,
                unassignedLabel: 'Unassigned',
                activeGroup,
                getDisplayName: () => 'Marketing',
            }),
        ).toEqual([
            activeGroup,
            {
                key: UNASSIGNED_DOMAIN_GROUP_KEY,
                label: 'Unassigned',
            },
        ]);
    });

    it('only resolves an active group for the document selected in the route', () => {
        const entityData = {
            urn: 'urn:li:document:active',
            domain: null,
        };

        expect(
            resolveActiveDocumentDomainGroup(entityData, 'urn:li:document:other', 'Unassigned', () => 'unused'),
        ).toBeUndefined();
        expect(resolveActiveDocumentDomainGroup(entityData, entityData.urn, 'Unassigned', () => 'unused')).toEqual({
            key: UNASSIGNED_DOMAIN_GROUP_KEY,
            label: 'Unassigned',
        });
        expect(
            resolveActiveDocumentDomainGroup(
                { urn: entityData.urn, domain },
                entityData.urn,
                'Unassigned',
                () => 'Marketing',
            ),
        ).toEqual({
            key: domain.urn,
            label: 'Marketing',
            entity: domain,
        });
    });

    it('builds searchDocuments filters for assigned and unassigned groups', () => {
        expect(buildDocumentDomainGroupSearchInput(domain.urn)).toEqual({ domains: [domain.urn] });
        expect(buildDocumentDomainGroupSearchInput(UNASSIGNED_DOMAIN_GROUP_KEY)).toEqual({ hasDomain: false });
    });

    it('checks whether a document belongs in a domain group', () => {
        const assigned = {
            domain: { domain: { urn: domain.urn, type: EntityType.Domain } },
        } as Document;
        const unassigned = { domain: null } as Document;

        expect(documentBelongsInDomainGroup(assigned, domain.urn)).toBe(true);
        expect(documentBelongsInDomainGroup(assigned, UNASSIGNED_DOMAIN_GROUP_KEY)).toBe(false);
        expect(documentBelongsInDomainGroup(unassigned, UNASSIGNED_DOMAIN_GROUP_KEY)).toBe(true);
        expect(documentBelongsInDomainGroup(null, domain.urn)).toBe(false);
    });

    it('moves the open document out of a group after its domain changes', () => {
        const moved = {
            urn: 'urn:li:document:moved',
            domain: { domain: { urn: domain.urn, type: EntityType.Domain } },
        } as Document;
        const other = { urn: 'urn:li:document:other', domain: null } as Document;

        expect(documentsInDomainGroup([moved, other], UNASSIGNED_DOMAIN_GROUP_KEY, moved)).toEqual([other]);
        expect(documentsInDomainGroup([other], domain.urn, moved)).toEqual([moved, other]);
        expect(documentsInDomainGroup([moved, other], domain.urn, moved)).toEqual([moved, other]);
    });

    it('refetches domain headers only when the open document changes groups', () => {
        expect(shouldRefetchDocumentDomainGroups(undefined, domain.urn)).toBe(false);
        expect(shouldRefetchDocumentDomainGroups(null, domain.urn)).toBe(false);
        expect(shouldRefetchDocumentDomainGroups(domain.urn, null)).toBe(false);
        expect(shouldRefetchDocumentDomainGroups(domain.urn, domain.urn)).toBe(false);
        expect(shouldRefetchDocumentDomainGroups(UNASSIGNED_DOMAIN_GROUP_KEY, domain.urn)).toBe(true);
    });

    it('reads sidebar title and parent breadcrumb helpers', () => {
        const doc = {
            urn: 'urn:li:document:1',
            info: { title: 'Runbook' },
            parentDocuments: {
                documents: [{ info: { title: 'Ops' } }, { info: { title: 'Root' } }],
            },
        } as Document;

        expect(getDocumentSidebarTitle(doc, 'Untitled')).toBe('Runbook');
        expect(getDocumentSidebarTitle({ urn: 'urn:li:document:2' } as Document, 'Untitled')).toBe('Untitled');
        expect(buildDocumentParentBreadcrumb(doc, 'Untitled')).toBe('Root > Ops');
        expect(buildDocumentParentBreadcrumb({ urn: 'urn:li:document:2' } as Document, 'Untitled')).toBeNull();
        expect(toDocumentGroupingEntityData(doc)).toEqual({ urn: doc.urn, domain: null });
        expect(toDocumentGroupingEntityData(null)).toBeNull();
    });
});
