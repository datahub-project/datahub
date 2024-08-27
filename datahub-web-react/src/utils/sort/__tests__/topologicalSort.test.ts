import { dataJob1, dataJob2, dataJob3 } from '../../../Mocks';
import { topologicalSort } from '../topologicalSort';

describe('topologicalSort', () => {
    it('sorts a list in correct order', () => {
        const sorted = topologicalSort([dataJob1, dataJob2, dataJob3]);
        expect(sorted?.[0]?.urn).toEqual(dataJob1.urn);
        expect(sorted?.[1]?.urn).toEqual(dataJob2.urn);
        expect(sorted?.[2]?.urn).toEqual(dataJob3.urn);
    });

    it('sorts a list in incorrect order', () => {
        const sorted = topologicalSort([dataJob3, dataJob1, dataJob2]);
        expect(sorted?.[0]?.urn).toEqual(dataJob1.urn);
        expect(sorted?.[1]?.urn).toEqual(dataJob2.urn);
        expect(sorted?.[2]?.urn).toEqual(dataJob3.urn);
    });
});
