import { useGetEntityWithSchema } from '@app/entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema';

export const useGetColumnTabCount = () => {
    // structuralOnly: this caller only needs the field count; skipping the full metadata
    // query avoids a duplicate network request when SchemaTab is simultaneously mounted.
    const { entityWithSchema, structuralSchemaMetadata, loading } = useGetEntityWithSchema(undefined, true);
    // Prefer full metadata field count; fall back to structural count so the badge is
    // populated as soon as the structural query resolves rather than showing 0 until
    // the full metadata query completes.
    const fieldsCount =
        entityWithSchema?.schemaMetadata?.fields?.length || structuralSchemaMetadata?.fields?.length || 0;

    return !loading ? fieldsCount : undefined;
};
