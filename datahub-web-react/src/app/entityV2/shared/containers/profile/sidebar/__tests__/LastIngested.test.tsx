import { green, orange, red } from '@ant-design/colors';
import moment from 'moment-timezone';
import { getLastIngestedColor } from '../LastIngested';

describe('getLastIngestedColor', () => {
    it('should return green if the last ingested date is the present moment', () => {
        const lastIngested = moment().valueOf();
        const lastIngestedColor = getLastIngestedColor(lastIngested);
        expect(lastIngestedColor).toBe(green[5]);
    });

    it('should return green if the last ingested date is less than a week ago from now', () => {
        const lastIngested = moment().subtract(1, 'day').valueOf();
        const lastIngestedColor = getLastIngestedColor(lastIngested);
        expect(lastIngestedColor).toBe(green[5]);
    });

    it('should return orange if the last ingested date is exactly a week ago', () => {
        const lastIngested = moment().subtract(1, 'week').valueOf();
        const lastIngestedColor = getLastIngestedColor(lastIngested);
        expect(lastIngestedColor).toBe(orange[5]);
    });

    it('should return orange if the last ingested date is more than a week but less than a month ago', () => {
        const lastIngested = moment().subtract(2, 'week').valueOf();
        const lastIngestedColor = getLastIngestedColor(lastIngested);
        expect(lastIngestedColor).toBe(orange[5]);
    });

    it('should return red if the last ingested date is exactly a month ago', () => {
        const lastIngested = moment().subtract(1, 'month').valueOf();
        const lastIngestedColor = getLastIngestedColor(lastIngested);
        expect(lastIngestedColor).toBe(red[5]);
    });

    it('should return red if the last ingested date is more than a month ago', () => {
        const lastIngested = moment().subtract(3, 'month').valueOf();
        const lastIngestedColor = getLastIngestedColor(lastIngested);
        expect(lastIngestedColor).toBe(red[5]);
    });
});
