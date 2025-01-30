import { CalendarData } from '../types';
import { getColorAccessor } from '../utils';

type Datum = {
    value1: number;
    value2: number;
};

const sampleData: CalendarData<Datum>[] = [
    { day: '2024-01-01', value: { value1: 10, value2: 10 } },
    { day: '2024-01-02', value: { value1: 20, value2: 15 } },
    { day: '2024-01-03', value: { value1: 0, value2: 0 } },
];

const DEFAULT_COLOR = '#EBECF0';

const sampleColorAccessors = getColorAccessor<Datum>(
    sampleData,
    {
        value1: {
            valueAccessor: (datum) => datum.value1,
            colors: ['#fff', '#000'], // white -> black
        },
        value2: {
            valueAccessor: (datum) => datum.value2,
            colors: ['#ff0000', '#00ff00', '#0000ff'], // red -> green -> blue
        },
    },
    DEFAULT_COLOR,
);

const sampleColorAccessorsEmpty = getColorAccessor<Datum>(sampleData, {}, DEFAULT_COLOR);

describe('getColorAccessors', () => {
    it('should return default color if there are zero values', () => {
        const result = sampleColorAccessors({ value1: 0, value2: 0 });

        expect(result).toBe(DEFAULT_COLOR);
    });

    it('should return defalut color when value is negative', () => {
        const result = sampleColorAccessors({ value1: -50, value2: -10 });

        expect(result).toBe(DEFAULT_COLOR);
    });

    it('should return defalut color when color accessors are no provided', () => {
        const result = sampleColorAccessorsEmpty({ value1: 10, value2: 10 });

        expect(result).toBe(DEFAULT_COLOR);
    });

    it('should return correctly interpolated color for average value', () => {
        const result = sampleColorAccessors({ value1: 10, value2: 0 });

        expect(result).toBe('rgb(128, 128, 128)'); // the color's between white and black
    });

    it('should return correctly interpolated color for max value', () => {
        const result = sampleColorAccessors({ value1: 20, value2: 0 });

        expect(result).toBe('rgb(0, 0, 0)');
    });

    it('should return colors for value2 when value2 great then value1', () => {
        const result = sampleColorAccessors({ value1: 10, value2: 20 });

        expect(result).toBe('rgb(0, 0, 255)'); // blue
    });

    it('should return colors for the first max value', () => {
        const result = sampleColorAccessors({ value1: 20, value2: 20 });

        expect(result).toBe('rgb(0, 0, 0)');
    });

    it('should return max color if value is great then max value', () => {
        const result = sampleColorAccessors({ value1: 50, value2: 20 });

        expect(result).toBe('rgb(0, 0, 0)');
    });
});
