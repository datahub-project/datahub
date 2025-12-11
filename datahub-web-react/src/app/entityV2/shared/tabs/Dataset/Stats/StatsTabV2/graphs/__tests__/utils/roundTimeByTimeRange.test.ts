/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { roundTimeByTimeRange } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';
import { TimeRange } from '@src/types.generated';

const MOCKED_NOW = 1739791888317;

describe('roundTimeByTimeRange', () => {
    it('should return correct value for Day of TimeRange', () => {
        const response = roundTimeByTimeRange(MOCKED_NOW, TimeRange.Day, true);

        expect(response).toEqual(1739750400000);
    });

    it('should return correct value for Week of TimeRange', () => {
        const response = roundTimeByTimeRange(MOCKED_NOW, TimeRange.Week, true);

        expect(response).toEqual(1739750400000);
    });

    it('should return correct value for Month of TimeRange', () => {
        const response = roundTimeByTimeRange(MOCKED_NOW, TimeRange.Month, true);

        expect(response).toEqual(1739750400000);
    });

    it('should return correct value for Quarter of TimeRange', () => {
        const response = roundTimeByTimeRange(MOCKED_NOW, TimeRange.Quarter, true);

        expect(response).toEqual(1739664000000);
    });

    it('should return correct value for HalfYear of TimeRange', () => {
        const response = roundTimeByTimeRange(MOCKED_NOW, TimeRange.HalfYear, true);

        expect(response).toEqual(1738368000000);
    });

    it('should return correct value for Year of TimeRange', () => {
        const response = roundTimeByTimeRange(MOCKED_NOW, TimeRange.Year, true);

        expect(response).toEqual(1738368000000);
    });

    it('should return the same value for All of TimeRange', () => {
        const response = roundTimeByTimeRange(MOCKED_NOW, TimeRange.All, true);

        expect(response).toEqual(MOCKED_NOW);
    });
});
