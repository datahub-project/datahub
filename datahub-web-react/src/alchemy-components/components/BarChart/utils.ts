import dayjs from 'dayjs';
import { COLOR_SCHEMES } from './constants';

export function generateMockData(length = 30, maxValue = 50_000, minValue = 0) {
    return Array(length)
        .fill(0)
        .map((_, index) => {
            const date = dayjs()
                .startOf('day')
                .add(index - length, 'days')
                .toDate()
                .getTime();
            const value = Math.max(Math.random() * maxValue, minValue);

            return {
                x: date,
                y: value,
            };
        });
}

export function generateMockDataHorizontal(length = 5, maxValue = 50_000, minValue = 0) {
    return Array(length)
        .fill(0)
        .map((_, index) => {
            return {
                y: index,
                x: Math.max(Math.random() * maxValue, minValue),
                colorScheme: COLOR_SCHEMES?.[index % (COLOR_SCHEMES.length - 1)],
                label: `Value-${index}${' text'.repeat(index)}`,
            };
        });
}

export function getMockedProps() {
    return {
        data: generateMockData(5),
        xAccessor: (datum) => datum.x,
        yAccessor: (datum) => Math.max(datum.y, 1000),
    };
}
