import { useGetEntityWithSchema } from '@app/entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema';

export const useGetColumnTabCount = () => {
    const { entityWithSchema, loading } = useGetEntityWithSchema();
    const fieldsCount = entityWithSchema?.schemaMetadata?.fields?.length || 0;

    return !loading ? fieldsCount : undefined;
};
