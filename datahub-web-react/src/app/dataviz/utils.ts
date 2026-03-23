// Number Abbreviations
export const abbreviateNumber = (str) => {
    const number = parseFloat(str);
    if (Number.isNaN(number)) return str;
    if (number < 1000) return number;
    const abbreviations = ['K', 'M', 'B', 'T'];
    const index = Math.floor(Math.log10(number) / 3);
    const suffix = abbreviations[index - 1];
    const shortNumber = number / 10 ** (index * 3);
    return `${shortNumber}${suffix}`;
};
