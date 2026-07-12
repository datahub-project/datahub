import { useFeatureFlag } from '@app/sharedV2/hooks/useFeatureFlag';

/**
 * Whether ingestion connection forms are generated from the connector's config
 * schema (the auto-generated <SchemaForm>) instead of the hand-written per-connector
 * RecipeForm. Gated by the `schemaDrivenFormsEnabled` feature flag (default off).
 */
export function useSchemaDrivenFormsEnabled(): boolean {
    return useFeatureFlag('schemaDrivenFormsEnabled');
}
