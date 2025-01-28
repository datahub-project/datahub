import { CalendarData } from '../types';
import { prepareCalendarData } from '../utils';

const sampleData: CalendarData<number>[] = [
    { day: '2024-12-02', value: 1 },
    { day: '2024-12-03', value: 2 },
    { day: '2024-12-04', value: 3 },
    { day: '2024-12-05', value: 4 },
    { day: '2024-12-06', value: 5 },
    { day: '2024-12-07', value: 6 },
    { day: '2024-12-08', value: 7 },
];

describe('prepareCalendarData', () => {
    it('should prepare data for a full month', () => {
        const result = prepareCalendarData(sampleData, '2024-12-01', '2024-12-31');

        // 3 months (Nov / Dec / Jan) because edge weeks aren't belong to Dec
        // because of there are less Dec days then of another month
        expect(result).toHaveLength(3);
        // Nov 1 week from 2024-11-25 to 2024-12-01
        expect(result[0].weeks).toHaveLength(1);
        expect(result[0].weeks[0].days[0].day).toBe('2024-11-25');
        expect(result[0].weeks[0].days[6].day).toBe('2024-12-01');
        expect(result[0].weeks[0].days.some((day) => day.value !== undefined)).toBeFalsy(); // no values
        // Dec 4 weeks from 2024-12-02 to 2024-12-29
        expect(result[1].weeks).toHaveLength(4);
        expect(result[1].weeks[0].days[0].day).toBe('2024-12-02');
        expect(result[1].weeks[3].days[6].day).toBe('2024-12-29');
        expect(result[1].weeks[0].days.some((day) => day.value !== undefined)).toBeTruthy(); // has values
        sampleData.forEach((datum, index) => {
            expect(result[1].weeks[0].days[index].value).toBe(datum.value);
        });
        expect(result[1].weeks[1].days.some((day) => day.value !== undefined)).toBeFalsy(); // no values
        expect(result[1].weeks[2].days.some((day) => day.value !== undefined)).toBeFalsy(); // no values
        expect(result[1].weeks[3].days.some((day) => day.value !== undefined)).toBeFalsy(); // no values
        // Jan 1 wekk from 2024-12-30 to 2025-01-05
        expect(result[2].weeks).toHaveLength(1);
        expect(result[2].weeks[0].days[0].day).toBe('2024-12-30');
        expect(result[2].weeks[0].days[6].day).toBe('2025-01-05');
        expect(result[2].weeks[0].days.some((day) => day.value !== undefined)).toBeFalsy(); // no values
    });

    it('should prepare data for a full week', () => {
        const result = prepareCalendarData(sampleData, '2024-12-02', '2024-12-08');

        expect(result).toHaveLength(1);
        expect(result[0]).toHaveProperty('weeks');
        expect(result[0].weeks).toHaveLength(1);
        expect(result[0].weeks[0].days).toHaveLength(7);
        sampleData.forEach((datum, index) => {
            expect(result[0].weeks[0].days[index].value).toBe(datum.value);
        });
    });

    it('should handle a month transition', () => {
        const result = prepareCalendarData(sampleData, '2024-11-04', '2024-12-29');

        // 2 months (Nov / Dec)
        expect(result).toHaveLength(2);
        // Nov 4 weeks from 2024-11-04 to 2024-12-01
        expect(result[0].key).toBe('2024-11');
        expect(result[0].weeks).toHaveLength(4);
        expect(result[0].weeks[0].days[0].day).toBe('2024-11-04');
        expect(result[0].weeks[3].days[6].day).toBe('2024-12-01');
        // Dec 4 weeks from 2024-12-02 to 2024-12-29
        expect(result[1].key).toBe('2024-12');
        expect(result[1].weeks).toHaveLength(4);
        expect(result[1].weeks[0].days[0].day).toBe('2024-12-02');
        expect(result[1].weeks[3].days[6].day).toBe('2024-12-29');
    });

    it('should handle a year transition', () => {
        const result = prepareCalendarData(sampleData, '2024-12-02', '2025-01-05');

        // 2 months (Dec / Jan)
        expect(result).toHaveLength(2);
        // Dec 4 weeks from 2024-12-02 to 2024-12-29
        expect(result[0].key).toBe('2024-12');
        expect(result[0].weeks).toHaveLength(4);
        expect(result[0].weeks[0].days[0].day).toBe('2024-12-02');
        expect(result[0].weeks[3].days[6].day).toBe('2024-12-29');
        // Jan 1 week from 2024-12-30 to 2025-01-05
        expect(result[1].key).toBe('2025-01');
        expect(result[1].weeks).toHaveLength(1);
        expect(result[1].weeks[0].days[0].day).toBe('2024-12-30');
        expect(result[1].weeks[0].days[6].day).toBe('2025-01-05');
    });

    it('should handle when the start and end date are the same', () => {
        const result = prepareCalendarData(sampleData, '2024-12-01', '2024-12-01');

        expect(result).toHaveLength(1);
        expect(result[0].weeks).toHaveLength(1);
        expect(result[0].weeks[0].days).toHaveLength(7); // rounding by start/end of week
        expect(result[0].weeks[0].days[0].day).toBe('2024-11-25');
        expect(result[0].weeks[0].days[6].day).toBe('2024-12-01');
    });

    it('should handle a range with no data for the given dates', () => {
        const result = prepareCalendarData([], '2024-01-01', '2024-01-07');

        expect(result).toHaveLength(1);
        expect(result[0]).toHaveProperty('weeks');
        expect(result[0].weeks).toHaveLength(1);
        expect(result[0].weeks[0].days).toHaveLength(7);
        expect(result[0].weeks[0].days.some((day) => day.value !== undefined)).toBeFalsy();
    });
});
