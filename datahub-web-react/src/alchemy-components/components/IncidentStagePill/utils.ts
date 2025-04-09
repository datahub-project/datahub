export const getCapitalizeWord = (word: string) =>
    word
        ?.toLowerCase()
        .replace(/_/g, ' ')
        .replace(/\b\w/g, (char) => char.toUpperCase());
