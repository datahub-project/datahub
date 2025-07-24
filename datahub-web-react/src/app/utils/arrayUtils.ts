export function mergeArraysOfObjects<T>(arrayA: Array<T>, arrayB: Array<T>, keyGetter: (item: T) => any): Array<T> {
    const keysFromArrayB = arrayB.map(keyGetter);
    return [...arrayA.filter((item) => !keysFromArrayB.includes(keyGetter(item))), ...arrayB];
}
