const intlFormat = (num) => {
    return new Intl.NumberFormat().format(Math.round(num * 10) / 10);
};

export const countFormatter: (num: number) => string = (num: number) => {
    if (num >= 1000000000) {
        return `${intlFormat(num / 1000000000)}B`;
    }
    if (num >= 1000000) {
        return `${intlFormat(num / 1000000)}M`;
    }

    if (num >= 1000) return `${intlFormat(num / 1000)}K`;

    return intlFormat(num);
};

export const countSeparator = (num) => {
    return num.toLocaleString();
};
