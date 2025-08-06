import { mergeSources } from '@app/ingestV2/source/cacheUtils';

describe('mergeSources', () => {
    const existingSources = [
        { urn: 'a', name: 'Source A', data: { foo: 1 } },
        { urn: 'b', name: 'Source B', data: { bar: 2 } },
    ];

    it('should replace the matching source when shouldReplace is true', () => {
        const updatedSource = { urn: 'a', name: 'Updated Source A', data: { foo: 99 } };
        const result = mergeSources(updatedSource, existingSources, true);
        expect(result).toEqual([updatedSource, existingSources[1]]);
    });

    it('should deep merge the matching source when shouldReplace is false', () => {
        const updatedSource = { urn: 'a', data: { foo: 42, newField: 100 } };
        const result = mergeSources(updatedSource, existingSources, false);
        expect(result[0]).toEqual({
            urn: 'a',
            name: 'Source A',
            data: { foo: 42, newField: 100 },
        });
        expect(result[1]).toEqual(existingSources[1]);
    });

    it('should add the updated source to the top if not found', () => {
        const updatedSource = { urn: 'c', name: 'Source C' };
        const result = mergeSources(updatedSource, existingSources, true);
        expect(result[0]).toEqual(updatedSource);
        expect(result).toHaveLength(3);
    });

    it('should work with an empty existingSources array', () => {
        const updatedSource = { urn: 'z', name: 'Source Z' };
        const result = mergeSources(updatedSource, [], true);
        expect(result).toEqual([updatedSource]);
    });

    it('should deep merge arrays by concatenation', () => {
        const sources = [{ urn: 'a', owners: ['foo'], data: { arr: [1, 2] } }];
        const updatedSource = { urn: 'a', owners: ['bar'], data: { arr: [3] } };
        const result = mergeSources(updatedSource, sources, false);
        expect(result[0].owners).toEqual(['foo', 'bar']);
        expect(result[0].data.arr).toEqual([1, 2, 3]);
    });
});
