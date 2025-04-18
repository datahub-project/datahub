import { roundTimeByTimeRange } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';
import { TimeRange } from '@src/types.generated';

const MOCKED_NOW = 1739791888317;

describe('roundTimeByTimeRange', () => {
    it('should return correct value for Day of TimeRange', () => {
        const response = roundTimeByTimeRange(MOCKED_NOW, TimeRange.Day);

        expect(response).toEqual(1739739600000);
    });

    it('should return correct value for Week of TimeRange', () => {
        const response = roundTimeByTimeRange(MOCKED_NOW, TimeRange.Week);

        expect(response).toEqual(1739739600000);
    });

    it('should return correct value for Month of TimeRange', () => {
        const response = roundTimeByTimeRange(MOCKED_NOW, TimeRange.Month);

        expect(response).toEqual(1739739600000);
    });

    it('should return correct value for Quarter of TimeRange', () => {
        const response = roundTimeByTimeRange(MOCKED_NOW, TimeRange.Quarter);

        expect(response).toEqual(1739653200000);
    });

    it('should return correct value for HalfYear of TimeRange', () => {
        const response = roundTimeByTimeRange(MOCKED_NOW, TimeRange.HalfYear);

        expect(response).toEqual(1738357200000);
    });

    it('should return correct value for Year of TimeRange', () => {
        const response = roundTimeByTimeRange(MOCKED_NOW, TimeRange.Year);

        expect(response).toEqual(1738357200000);
    });

    it('should return the same value for All of TimeRange', () => {
        const response = roundTimeByTimeRange(MOCKED_NOW, TimeRange.All);

        expect(response).toEqual(MOCKED_NOW);
    });
});
