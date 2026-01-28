import { getCalendarStartTimeByTimeRange } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';
import { TimeRange } from '@src/types.generated';

const MOCKED_NOW = 1739791888317;

describe('getCalendarStartTimeByTimeRange', () => {
    it('should return correct value for Day of TimeRange', () => {
        const response = getCalendarStartTimeByTimeRange(MOCKED_NOW, TimeRange.Day, true);

        expect(response).toEqual(1739705488317);
    });

    it('should return correct value for Week of TimeRange', () => {
        const response = getCalendarStartTimeByTimeRange(MOCKED_NOW, TimeRange.Week, true);

        expect(response).toEqual(1739187088317);
    });

    it('should return correct value for Month of TimeRange', () => {
        const response = getCalendarStartTimeByTimeRange(MOCKED_NOW, TimeRange.Month, true);

        expect(response).toEqual(1737113488317);
    });

    it('should return correct value for Quarter of TimeRange', () => {
        const response = getCalendarStartTimeByTimeRange(MOCKED_NOW, TimeRange.Quarter, true);

        expect(response).toEqual(1731843088317);
    });

    it('should return correct value for HalfYear of TimeRange', () => {
        const response = getCalendarStartTimeByTimeRange(MOCKED_NOW, TimeRange.HalfYear, true);

        expect(response).toEqual(1723894288317);
    });

    it('should return correct value for Year of TimeRange', () => {
        const response = getCalendarStartTimeByTimeRange(MOCKED_NOW, TimeRange.Year, true);

        expect(response).toEqual(1708169488317);
    });

    it('should return undefined value for All of TimeRange', () => {
        const response = getCalendarStartTimeByTimeRange(MOCKED_NOW, TimeRange.All, true);

        expect(response).toEqual(undefined);
    });
});
