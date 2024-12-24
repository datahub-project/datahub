export const formatBadgeValue = (value: number, overflowCount?: number): string => {
    if (overflowCount === undefined || value < overflowCount) return String(value);

    return `${overflowCount}+`;
};

export function omitKeys<T extends object, K extends keyof T>(obj: T, keys: K[]): Omit<T, K> {
    const { ...rest } = obj;

    keys.forEach((key) => {
        delete rest[key];
    });

    return rest;
}
