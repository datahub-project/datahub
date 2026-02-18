import moment from 'moment-timezone';

import { getLastIngestedColor } from '@app/entityV2/shared/containers/profile/sidebar/LastIngested';

const mockColors = {
    textSuccess: '#textSuccess',
    textWarning: '#textWarning',
    textError: '#textError',
};

describe('getLastIngestedColor', () => {
    it('should return green if the last ingested date is the present moment', () => {
        const lastIngested = moment().valueOf();
        const lastIngestedColor = getLastIngestedColor(lastIngested, mockColors);
        expect(lastIngestedColor).toBe(mockColors.textSuccess);
    });

    it('should return green if the last ingested date is less than a week ago from now', () => {
        const lastIngested = moment().subtract(1, 'day').valueOf();
        const lastIngestedColor = getLastIngestedColor(lastIngested, mockColors);
        expect(lastIngestedColor).toBe(mockColors.textSuccess);
    });

    it('should return orange if the last ingested date is exactly a week ago', () => {
        const lastIngested = moment().subtract(1, 'week').valueOf();
        const lastIngestedColor = getLastIngestedColor(lastIngested, mockColors);
        expect(lastIngestedColor).toBe(mockColors.textWarning);
    });

    it('should return orange if the last ingested date is more than a week but less than a month ago', () => {
        const lastIngested = moment().subtract(2, 'week').valueOf();
        const lastIngestedColor = getLastIngestedColor(lastIngested, mockColors);
        expect(lastIngestedColor).toBe(mockColors.textWarning);
    });

    it('should return red if the last ingested date is exactly a month ago', () => {
        const lastIngested = moment().subtract(1, 'month').valueOf();
        const lastIngestedColor = getLastIngestedColor(lastIngested, mockColors);
        expect(lastIngestedColor).toBe(mockColors.textError);
    });

    it('should return red if the last ingested date is more than a month ago', () => {
        const lastIngested = moment().subtract(3, 'month').valueOf();
        const lastIngestedColor = getLastIngestedColor(lastIngested, mockColors);
        expect(lastIngestedColor).toBe(mockColors.textError);
    });
});
