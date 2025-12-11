/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { dataJob1, dataJob2, dataJob3 } from '@src/Mocks';
import { topologicalSort } from '@utils/sort/topologicalSort';

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
