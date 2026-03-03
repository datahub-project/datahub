// Number Abbreviations
export const abbreviateNumber = (str) => {
    const number = parseFloat(str);
    if (Number.isNaN(number)) return str;
    const sign = number < 0 ? '-' : '';
    const absoluteNumber = Math.abs(number);
    if (absoluteNumber < 1000) return number;
    const abbreviations = ['K', 'M', 'B', 'T'];
    const index = Math.floor(Math.log10(absoluteNumber) / 3);
    const suffix = abbreviations[index - 1];
    const shortNumber = absoluteNumber / 10 ** (index * 3);
    return `${sign}${shortNumber}${suffix}`;
};
