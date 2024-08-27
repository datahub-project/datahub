type NumMapType = Record<'billion' | 'million' | 'thousand', { value: number; symbol: string }>;

const NumMap: NumMapType = {
    billion: {
        value: 1000000000,
        symbol: 'B',
    },
    million: {
        value: 1000000,
        symbol: 'M',
    },
    thousand: {
        value: 1000,
        symbol: 'K',
    },
} as const;

const isBillions = (num: number) => num >= NumMap.billion.value;
const isMillions = (num: number) => num >= NumMap.million.value;
const isThousands = (num: number) => num >= NumMap.thousand.value;

const intlFormat = (num: number) => new Intl.NumberFormat().format(Math.round(num * 10) / 10);

export const needsFormatting = (num: number) => isThousands(num);

export const countFormatter = (num: number) => {
    if (isBillions(num)) return `${intlFormat(num / NumMap.billion.value)}${NumMap.billion.symbol}`;
    if (isMillions(num)) return `${intlFormat(num / NumMap.million.value)}${NumMap.million.symbol}`;
    if (isThousands(num)) return `${intlFormat(num / NumMap.thousand.value)}${NumMap.thousand.symbol}`;
    return intlFormat(num);
};
