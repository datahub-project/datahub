import { combineEntityDataWithSiblings } from '@app/entity/shared/siblingUtils';

// GraphQL results can expose nulls as non-configurable properties. clean() then cannot
// delete them, and deepmerge lets the null replace the other sibling's value.
function withNonConfigurableNulls<T extends object>(record: T, keys: (keyof T)[]): T {
    const copy = { ...record };
    keys.forEach((key) => {
        Object.defineProperty(copy, key, {
            value: null,
            enumerable: true,
            writable: true,
            configurable: false,
        });
    });
    return copy;
}

function combineViewing(pageEntity: object, siblingEntity: object, pageIsPrimary: boolean) {
    const combined = combineEntityDataWithSiblings({
        dataset: {
            urn: 'urn:li:dataset:page',
            siblings: { isPrimary: pageIsPrimary },
            siblingsSearch: {
                count: 1,
                searchResults: [
                    {
                        entity: {
                            urn: 'urn:li:dataset:sibling',
                            siblings: { isPrimary: !pageIsPrimary },
                            ...siblingEntity,
                        },
                    },
                ],
            },
            ...pageEntity,
        },
    });
    return combined.dataset as any;
}

describe('sibling null property merge', () => {
    it('keeps warehouse property values when the primary sibling has nulls', () => {
        const page = {
            properties: {
                name: 'events',
                description: 'loaded from the warehouse',
                qualifiedName: 'proj.analytics.events',
                created: 1793591629320,
                createdActor: 'urn:li:corpuser:bq-loader',
                lastModified: { time: 1793591700000, actor: 'urn:li:corpuser:bq-loader' },
            },
            editableProperties: {
                description: 'warehouse note',
            },
        };
        const primarySibling = {
            properties: withNonConfigurableNulls(
                {
                    name: 'events',
                    description: 'model docs',
                    qualifiedName: 'analytics.events',
                },
                ['created', 'createdActor', 'lastModified'],
            ),
            editableProperties: withNonConfigurableNulls({}, ['description']),
        };

        const result = combineViewing(page, primarySibling, false);

        expect(result.properties.created).toBe(1793591629320);
        expect(result.properties.createdActor).toBe('urn:li:corpuser:bq-loader');
        expect(result.properties.lastModified).toEqual({
            time: 1793591700000,
            actor: 'urn:li:corpuser:bq-loader',
        });
        expect(result.properties.description).toBe('model docs');
        expect(result.properties.qualifiedName).toBe('analytics.events');
        expect(result.editableProperties.description).toBe('warehouse note');
    });

    it('keeps a real primary value ahead of the other sibling', () => {
        const primary = {
            properties: {
                name: 'events',
                description: 'model docs',
                qualifiedName: 'analytics.events',
                created: 111,
                createdActor: 'urn:li:corpuser:dbt',
                lastModified: { time: 222, actor: 'urn:li:corpuser:dbt' },
            },
        };
        const sibling = {
            properties: {
                name: 'events',
                description: 'loaded from the warehouse',
                qualifiedName: 'proj.analytics.events',
                created: 1793591629320,
                createdActor: 'urn:li:corpuser:bq-loader',
                lastModified: { time: 1793591700000, actor: 'urn:li:corpuser:bq-loader' },
            },
        };

        const result = combineViewing(primary, sibling, true);

        expect(result.properties.created).toBe(111);
        expect(result.properties.createdActor).toBe('urn:li:corpuser:dbt');
        expect(result.properties.description).toBe('model docs');
        expect(result.properties.qualifiedName).toBe('analytics.events');
        // lastModified already has its own merge, and that merge keeps the other sibling
        // when that sibling has a timestamp. A null stamp still falls back in the test above.
        expect(result.properties.lastModified).toEqual({
            time: 1793591700000,
            actor: 'urn:li:corpuser:bq-loader',
        });
    });

    it('falls back when a nullable primary property was deleted before merge', () => {
        const primary = {
            properties: {
                name: 'events',
                description: null,
                qualifiedName: null,
                created: null,
                createdActor: null,
                lastModified: null,
            },
        };
        const sibling = {
            properties: {
                name: 'events',
                description: 'loaded from the warehouse',
                qualifiedName: 'proj.analytics.events',
                created: 1793591629320,
                createdActor: 'urn:li:corpuser:bq-loader',
                lastModified: { time: 1793591700000, actor: 'urn:li:corpuser:bq-loader' },
            },
        };

        const result = combineViewing(primary, sibling, true);

        expect(result.properties.created).toBe(1793591629320);
        expect(result.properties.createdActor).toBe('urn:li:corpuser:bq-loader');
        expect(result.properties.description).toBe('loaded from the warehouse');
        expect(result.properties.qualifiedName).toBe('proj.analytics.events');
        expect(result.properties.lastModified.time).toBe(1793591700000);
    });
});
