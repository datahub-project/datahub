import { useGetEntityWithSchema } from '@app/entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema';

export const useGetColumnTabCount = () => {
    // structuralOnly: this caller only needs the field count; skipping the full metadata
    // query avoids a duplicate network request when SchemaTab is simultaneously mounted.
    const { entityWithSchema, structuralSchemaMetadata, loading } = useGetEntityWithSchema(undefined, true);
    // entityWithSchema carries the schema only when the entity context already had it (no
    // query fires at all); otherwise structuralOnly means the count comes from the lean
    // structural query, which is all this badge needs.
    const fieldsCount =
        entityWithSchema?.schemaMetadata?.fields?.length || structuralSchemaMetadata?.fields?.length || 0;

    return !loading ? fieldsCount : undefined;
};
