type OperationVariables = Record<string, unknown>;

/**
 * Global search flags are applied on the way out so every operation sees them.
 * An explicit `skipLineage: true` / `skipSiblingsSearch: true` is kept: callers that opt out
 * (search page for lineage; deprecation/hydration for siblings) must not be overwritten when
 * the corresponding feature flag is off. The flags still force a skip when on.
 */
export function injectGlobalSearchVariables(
    variables: OperationVariables,
    flags: { showSeparateSiblings: boolean; hideLineageInSearchCards: boolean },
): OperationVariables {
    return {
        ...variables,
        skipSiblingsSearch: flags.showSeparateSiblings || variables.skipSiblingsSearch === true,
        skipLineage: flags.hideLineageInSearchCards || variables.skipLineage === true,
    };
}
