import { GenericEntityProperties } from '@app/entity/shared/types';
import {
    getStructuredPropertyRows,
    identifyAndAddParentRows,
} from '@app/entityV2/shared/tabs/Properties/useStructuredProperties';

const PROPAGATED = { attribution: { sourceDetail: [{ key: 'propagated', value: 'true' }] } };

const structuredProperty = (urn: string, propagated = false) => ({
    structuredProperty: { urn, exists: true, definition: { displayName: urn, qualifiedName: urn, valueType: {} } },
    values: [],
    ...(propagated && PROPAGATED),
});

const entityWith = (properties: unknown[]) =>
    ({ structuredProperties: { properties } }) as unknown as GenericEntityProperties;

describe('getStructuredPropertyRows deduplication', () => {
    it('collapses duplicate structured property urns to a single row', () => {
        const rows = getStructuredPropertyRows(
            entityWith([
                structuredProperty('urn:li:structuredProperty:p'),
                structuredProperty('urn:li:structuredProperty:p'),
            ]),
        );
        expect(rows).toHaveLength(1);
        expect(rows[0].structuredProperty?.urn).toBe('urn:li:structuredProperty:p');
    });

    it('prefers the user-applied entry over a propagated duplicate', () => {
        const rows = getStructuredPropertyRows(
            entityWith([
                structuredProperty('urn:li:structuredProperty:p', true),
                structuredProperty('urn:li:structuredProperty:p'),
            ]),
        );
        expect(rows).toHaveLength(1);
        expect(rows[0].attribution).toBeUndefined();
    });

    it('still keeps a propagated property that has no duplicate', () => {
        const rows = getStructuredPropertyRows(entityWith([structuredProperty('urn:li:structuredProperty:p', true)]));
        expect(rows).toHaveLength(1);
        expect(rows[0].attribution).toBeDefined();
    });
});

describe('identifyAndAddParentRows', () => {
    it('should not return parent rows when there are none', () => {
        const propertyRows = [
            { displayName: 'test1', qualifiedName: 'test1' },
            { displayName: 'test2', qualifiedName: 'test2' },
        ];
        expect(identifyAndAddParentRows(propertyRows)).toMatchObject([]);
    });

    it('should not return parent rows when another row starts with the same letters but is a different token', () => {
        const propertyRows = [
            { displayName: 'test1', qualifiedName: 'testing.one' },
            { displayName: 'test2', qualifiedName: 'testingAgain.two' },
        ];
        expect(identifyAndAddParentRows(propertyRows)).toMatchObject([]);
    });

    it('should return parent rows properly', () => {
        const propertyRows = [
            { displayName: 'test1', qualifiedName: 'testing.one' },
            { displayName: 'test2', qualifiedName: 'testing.two' },
            { displayName: 'test3', qualifiedName: 'testing.three' },
        ];
        expect(identifyAndAddParentRows(propertyRows)).toMatchObject([
            { displayName: 'testing', qualifiedName: 'testing', childrenCount: 3 },
        ]);
    });

    it('should return parent rows properly with multiple layers of nesting', () => {
        const propertyRows = [
            { displayName: 'test1', qualifiedName: 'testing.one.two.a.1' },
            { displayName: 'test1', qualifiedName: 'testing.one.two.a.2' },
            { displayName: 'test1', qualifiedName: 'testing.one.two.b' },
            { displayName: 'test1', qualifiedName: 'testing.one.three' },
            { displayName: 'test2', qualifiedName: 'testing.two.c.d' },
            { displayName: 'test3', qualifiedName: 'testing.three' },
            { displayName: 'test3', qualifiedName: 'testParent' },
        ];
        expect(identifyAndAddParentRows(propertyRows)).toMatchObject([
            { displayName: 'testing', qualifiedName: 'testing', isParentRow: true, childrenCount: 6 },
            { displayName: 'testing.one', qualifiedName: 'testing.one', isParentRow: true, childrenCount: 4 },
            { displayName: 'testing.one.two', qualifiedName: 'testing.one.two', isParentRow: true, childrenCount: 3 },
            {
                displayName: 'testing.one.two.a',
                qualifiedName: 'testing.one.two.a',
                isParentRow: true,
                childrenCount: 2,
            },
        ]);
    });

    it('should return parent rows properly with multiple layers of nesting regardless of order', () => {
        const propertyRows = [
            { displayName: 'test1', qualifiedName: 'testing.one.two.a.1' },
            { displayName: 'test3', qualifiedName: 'testParent' },
            { displayName: 'test1', qualifiedName: 'testing.one.three' },
            { displayName: 'test2', qualifiedName: 'testing.two.c.d' },
            { displayName: 'test1', qualifiedName: 'testing.one.two.b' },
            { displayName: 'test3', qualifiedName: 'testing.three' },
            { displayName: 'test1', qualifiedName: 'testing.one.two.a.2' },
        ];
        expect(identifyAndAddParentRows(propertyRows)).toMatchObject([
            { displayName: 'testing', qualifiedName: 'testing', isParentRow: true, childrenCount: 6 },
            { displayName: 'testing.one', qualifiedName: 'testing.one', isParentRow: true, childrenCount: 4 },
            { displayName: 'testing.one.two', qualifiedName: 'testing.one.two', isParentRow: true, childrenCount: 3 },
            {
                displayName: 'testing.one.two.a',
                qualifiedName: 'testing.one.two.a',
                isParentRow: true,
                childrenCount: 2,
            },
        ]);
    });

    it('should return parent rows properly with simpler layers of nesting', () => {
        const propertyRows = [
            { displayName: 'test2', qualifiedName: 'testing.two.c.d' },
            { displayName: 'test3', qualifiedName: 'testing.three' },
            { displayName: 'test3', qualifiedName: 'testParent' },
        ];
        expect(identifyAndAddParentRows(propertyRows)).toMatchObject([
            { displayName: 'testing', qualifiedName: 'testing', isParentRow: true, childrenCount: 2 },
        ]);
    });
});
