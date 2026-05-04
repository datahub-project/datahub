import { parseJsonPropsToRows } from '@app/entityV2/shared/tabs/Properties/utils';

describe('parseJsonPropsToRows', () => {
    it('returns empty array for null or undefined', () => {
        expect(parseJsonPropsToRows(null)).toEqual([]);
        expect(parseJsonPropsToRows(undefined)).toEqual([]);
        expect(parseJsonPropsToRows('')).toEqual([]);
    });

    it('parses valid jsonProps into PropertyRows', () => {
        const jsonProps = JSON.stringify({
            'iceberg.field.id': '1',
            'iceberg.field.optional': 'false',
        });
        const rows = parseJsonPropsToRows(jsonProps);
        expect(rows).toHaveLength(2);
        expect(rows[0]).toMatchObject({
            displayName: 'iceberg.field.id',
            qualifiedName: '__jsonProp__iceberg.field.id',
            values: [{ value: '1', entity: null }],
        });
        expect(rows[1]).toMatchObject({
            displayName: 'iceberg.field.optional',
            qualifiedName: '__jsonProp__iceberg.field.optional',
            values: [{ value: 'false', entity: null }],
        });
    });

    it('filters by key when filterText is provided', () => {
        const jsonProps = JSON.stringify({ nullAllowed: 'false', otherProp: 'value' });
        const rows = parseJsonPropsToRows(jsonProps, 'null');
        expect(rows).toHaveLength(1);
        expect(rows[0].displayName).toBe('nullAllowed');
    });

    it('filters by value when filterText matches value', () => {
        const jsonProps = JSON.stringify({ prop1: 'hello', prop2: 'world' });
        const rows = parseJsonPropsToRows(jsonProps, 'world');
        expect(rows).toHaveLength(1);
        expect(rows[0].displayName).toBe('prop2');
    });

    it('filter is case-insensitive', () => {
        const jsonProps = JSON.stringify({ NullAllowed: 'False' });
        expect(parseJsonPropsToRows(jsonProps, 'nullallowed')).toHaveLength(1);
        expect(parseJsonPropsToRows(jsonProps, 'FALSE')).toHaveLength(1);
    });

    it('returns empty array and warns on malformed JSON', () => {
        const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const rows = parseJsonPropsToRows('{not valid json}');
        expect(rows).toEqual([]);
        expect(warnSpy).toHaveBeenCalledWith('Failed to parse jsonProps for schema field:', expect.any(Error));
        warnSpy.mockRestore();
    });

    it('prefixes qualifiedName to avoid rowKey collisions with structured properties', () => {
        const jsonProps = JSON.stringify({ 'some.key': 'value' });
        const rows = parseJsonPropsToRows(jsonProps);
        expect(rows[0].qualifiedName).toBe('__jsonProp__some.key');
        expect(rows[0].qualifiedName).not.toBe('some.key');
    });

    it('returns empty array for valid JSON that is not an object (array)', () => {
        expect(parseJsonPropsToRows('["a", "b"]')).toEqual([]);
    });

    it('returns empty array for valid JSON that is not an object (primitive)', () => {
        expect(parseJsonPropsToRows('42')).toEqual([]);
        expect(parseJsonPropsToRows('"just a string"')).toEqual([]);
        expect(parseJsonPropsToRows('null')).toEqual([]);
    });
});
