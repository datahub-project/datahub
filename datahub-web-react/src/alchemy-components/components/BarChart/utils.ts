import dayjs from 'dayjs';

export function generateMockData(length = 30, maxValue = 50_000, minValue = 0) {
    return Array(length)
        .fill(0)
        .map((_, index) => {
            const date = dayjs()
                .startOf('day')
                .add(index - length, 'days')
                .toDate();
            const value = Math.max(Math.random() * maxValue, minValue);

            return {
                x: date,
                y: value,
            };
        });
}

export function getMockedProps() {
    return {
        data: generateMockData(),
        xAccessor: (datum) => datum.x,
        yAccessor: (datum) => Math.max(datum.y, 1000),
    };
}
