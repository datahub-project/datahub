import { describe, expect, it } from 'vitest';

import {
    ALL_RESOURCE_TYPES,
    filterResourceGroups,
    getResourceTypeOptions,
    groupResources,
} from '@app/entityV2/shared/tabs/Documentation/components/ResourcesModal.utils';

import { Document, InstitutionalMemoryMetadata } from '@types';

const documentItem = (title: string) => ({
    type: 'document' as const,
    data: { urn: `urn:li:document:${title}`, info: { title } } as Document,
    sortLabel: title,
});

const linkItem = (label: string, url: string) => ({
    type: 'link' as const,
    data: { label, description: label, url } as InstitutionalMemoryMetadata,
    sortLabel: label,
});

describe('ResourcesModal utilities', () => {
    const labels = { documents: 'Documents', links: 'Links' };

    it('groups resources into documents and links', () => {
        const groups = groupResources(
            [
                documentItem('Deploy service'),
                documentItem('Restore service'),
                documentItem('Overview'),
                linkItem('Dashboard', 'https://example.com'),
            ],
            labels,
        );

        expect(groups.map(({ key, label, items }) => ({ key, label, count: items.length }))).toEqual([
            { key: 'document', label: 'Documents', count: 3 },
            { key: 'link', label: 'Links', count: 1 },
        ]);
    });

    it('exposes documents and links as type filter options', () => {
        const options = getResourceTypeOptions(
            [
                documentItem('Deploy service'),
                documentItem('Restore service'),
                documentItem('Overview'),
                linkItem('Dashboard', 'https://example.com'),
            ],
            labels,
        );

        expect(options).toEqual([
            { value: 'document', label: 'Documents' },
            { value: 'link', label: 'Links' },
        ]);
    });

    it('searches resource labels and link URLs case-insensitively', () => {
        const groups = groupResources(
            [documentItem('Deploy service'), linkItem('Metrics', 'https://example.com/dashboard')],
            labels,
        );

        expect(filterResourceGroups(groups, ALL_RESOURCE_TYPES, 'DEPLOY')?.[0]?.items).toHaveLength(1);
        expect(filterResourceGroups(groups, ALL_RESOURCE_TYPES, 'dashboard')?.[0]?.key).toBe('link');
        expect(filterResourceGroups(groups, ALL_RESOURCE_TYPES, 'missing')).toEqual([]);
    });

    it('filters to one resource type', () => {
        const groups = groupResources(
            [documentItem('Deploy service'), linkItem('Dashboard', 'https://example.com')],
            labels,
        );

        expect(filterResourceGroups(groups, 'link', '')).toEqual([groups[1]]);
    });

    it('applies type and search filters together', () => {
        const groups = groupResources(
            [documentItem('Dashboard document'), linkItem('Dashboard link', 'https://example.com')],
            labels,
        );

        const result = filterResourceGroups(groups, 'document', 'dashboard');

        expect(result).toHaveLength(1);
        expect(result[0]?.key).toBe('document');
        expect(result[0]?.items.map((item) => item.sortLabel)).toEqual(['Dashboard document']);
    });
});
