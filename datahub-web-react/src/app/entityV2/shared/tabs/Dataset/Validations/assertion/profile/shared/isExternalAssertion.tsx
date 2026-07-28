/**
 * Returns true if the assertion is an external assertion, meaning it was reported using a custom
 * source or a well-supported integration like dbt test or Great Expectations.
 *
 * In OSS every assertion is external: assertions are produced via the API / ingestion sources
 * rather than natively created and executed by DataHub. Native and inferred ("smart") assertions
 * are DataHub Cloud only, so this always returns true here.
 */
export const isExternalAssertion = (_assertion?: unknown): boolean => {
    return true;
};
