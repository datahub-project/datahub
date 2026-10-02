type OperationVariables = Record<string, unknown>;

/**
 * Global search flags are applied on the way out so every operation sees them.
 * An explicit `skipLineage: true` is kept: callers such as the search page opt out of the
 * per-result lineage counts even when the hide-lineage flag is off. The flag still forces a skip.
 */
export function injectGlobalSearchVariables(
    variables: OperationVariables,
    flags: { showSeparateSiblings: boolean; hideLineageInSearchCards: boolean },
): OperationVariables {
    return {
        ...variables,
        skipSiblingsSearch: flags.showSeparateSiblings,
        skipLineage: flags.hideLineageInSearchCards || variables.skipLineage === true,
    };
}
