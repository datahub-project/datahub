import {
    UNASSIGNED_DOMAIN_GROUP_KEY,
    buildDocumentDomainGroupSearchInput,
    buildDocumentDomainGroups,
    buildDocumentParentBreadcrumb,
    documentBelongsInDomainGroup,
    getDocumentDomainGroupField,
    getDocumentSidebarTitle,
    isDocumentGroupByValue,
    isUnassignedDocumentDomainGroup,
    resolveActiveDocumentDomainGroup,
    sumFacetAggregationCounts,
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

    it('sums facet counts for the unassigned group', () => {
        const facet = {
            aggregations: [{ value: EntityType.Document, count: 4 }],
        } as FacetMetadata;

        expect(sumFacetAggregationCounts(facet)).toBe(4);
        expect(sumFacetAggregationCounts()).toBe(0);
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
