export enum MergeStrategy {
    preferAItems = 'preferAItems',
    preferBItems = 'preferBItems',
}

export function mergeArrays<T>(
    arrayA: Array<T>,
    arrayB: Array<T>,
    keyGetter: (item: T) => any = (item) => item,
    strategy: MergeStrategy = MergeStrategy.preferAItems,
): Array<T> {
    const keysOfArrayA = arrayA.map(keyGetter);
    const keysOfArrayB = arrayB.map(keyGetter);

    switch (strategy) {
        case MergeStrategy.preferAItems:
            return [...arrayA, ...arrayB.filter((item) => !keysOfArrayA.includes(keyGetter(item)))];
        case MergeStrategy.preferBItems:
        default:
            return [...arrayA.filter((item) => !keysOfArrayB.includes(keyGetter(item))), ...arrayB];
    }
}
