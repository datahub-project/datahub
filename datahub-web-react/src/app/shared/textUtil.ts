export function capitalizeFirstLetter(str?: string | null) {
    if (!str) {
        return undefined;
    }
    return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
}

export function lowerFirstLetter(str?: string | null) {
    if (!str) {
        return undefined;
    }
    return str.charAt(0).toLowerCase() + str.slice(1);
}

export function capitalizeFirstLetterOnly(str?: string | null) {
    if (!str) {
        return undefined;
    }
    return str.charAt(0).toUpperCase() + str.slice(1);
}

export function validateCustomUrnId(str: string) {
    if (str.indexOf(' ') > 0) return false;
    if (str.indexOf(',') > 0) return false;
    if (str.indexOf('(') > 0) return false;
    if (str.indexOf(')') > 0) return false;
    if (str.indexOf(':') > 0) return false;
    return true;
}

export function pluralize(count: number, noun: string, suffix = 's') {
    return count !== 1 ? pluralizeIfIrregular(noun, suffix) : noun;
}

export function forcePluralize(noun: string, suffix = 's') {
    return `${noun}${suffix}`;
}

export function pluralizeIfIrregular(noun: string, suffix = 's'): string {
    const irregularPlurals: Record<string, string> = {
        query: 'queries',
    };

    if (irregularPlurals.hasOwnProperty(noun?.toLowerCase())) {
        return irregularPlurals[noun?.toLowerCase()];
    }
    return `${noun}${suffix}`;
}
