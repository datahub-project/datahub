type SelectQueriesListDataArgs<T> = {
    data: T | undefined;
    previousData: T | undefined;
    error: unknown;
    entityKey: string;
    loadedEntityKey: string | undefined;
};

/**
 * Picks the listQueries payload to render.
 *
 * Paging and filtering clear `data` under `cache-first` until the next response arrives.
 * The previous payload can fill that gap for the same dataset. It must not fill the gap
 * after the dataset changes, or after the request fails, or the tab keeps showing queries
 * the user can still edit or delete.
 */
export function selectQueriesListData<T>({
    data,
    previousData,
    error,
    entityKey,
    loadedEntityKey,
}: SelectQueriesListDataArgs<T>): T | undefined {
    if (data !== undefined) {
        return data;
    }
    if (error || loadedEntityKey !== entityKey) {
        return undefined;
    }
    return previousData;
}

export function queriesEntityKey(entityUrn?: string, siblingUrn?: string): string {
    return `${entityUrn ?? ''}\0${siblingUrn ?? ''}`;
}
