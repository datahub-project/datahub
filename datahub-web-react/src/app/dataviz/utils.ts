// IEEE-754 doubles carry ~16 significant decimal digits, and arithmetic like
// `2713.2 / 1000` lands 1 ULP off its intended value (= 2.7131999999999996), so
// String() prints the full noisy tail. Rounding to this many significant figures
// strips that noise while preserving far more precision than any abbreviated
// label needs — it sits safely below the ~16-digit noise floor and well above the
// handful of figures a real tick label uses.
const ABBREVIATION_SIGNIFICANT_DIGITS = 12;

// Number Abbreviations
export const abbreviateNumber = (str) => {
    const number = parseFloat(str);
    if (Number.isNaN(number)) return str;
    if (number < 1000) return Number(number.toPrecision(ABBREVIATION_SIGNIFICANT_DIGITS));
    const abbreviations = ['K', 'M', 'B', 'T'];
    const index = Math.floor(Math.log10(number) / 3);
    const suffix = abbreviations[index - 1];
    const shortNumber = number / 10 ** (index * 3);
    return `${Number(shortNumber.toPrecision(ABBREVIATION_SIGNIFICANT_DIGITS))}${suffix}`;
};
