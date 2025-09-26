export function mergeArraysOfObjects<T>(
    arrayA: Array<T>,
    arrayB: Array<T>,
    keyGetter: (item: T) => any,
    preferOriginalOrdering = false,
): Array<T> {
    if (preferOriginalOrdering) {
        const keysFromArrayA = arrayA.map(keyGetter);
        return [...arrayA, ...arrayB.filter((item) => !keysFromArrayA.includes(keyGetter(item)))];
    }

    const keysFromArrayB = arrayB.map(keyGetter);
    return [...arrayA.filter((item) => !keysFromArrayB.includes(keyGetter(item))), ...arrayB];
}
