import { useGlobalSettingsContext } from '@app/context/GlobalSettings/GlobalSettingsContext';
import { INFER_DOCUMENTATION_ENTITY_TYPES } from '@app/entityV2/shared/components/inferredDocs/constants';
import { useInferDocumentationMutation } from '@src/graphql/mutations.generated';
import { EntityType } from '@src/types.generated';

export const useInferDocumentationForItem = ({
    entityUrn,
    columnPath,
    saveResult,
}: {
    entityUrn: string;
    columnPath?: string;
    saveResult?: boolean;
}) => {
    const [inferDocumentation] = useInferDocumentationMutation();

    return async (): Promise<string | undefined> => {
        const result = await inferDocumentation({
            variables: {
                urn: entityUrn,
                saveResult: !!saveResult,
            },
        });
        const maybeColumnsJSON = result.data?.inferDocumentation?.columnDescriptions?.jsonBlob;
        const columnDescr = maybeColumnsJSON ? JSON.parse(maybeColumnsJSON) : {};
        return !columnPath ? result.data?.inferDocumentation?.entityDescription : columnDescr[columnPath];
    };
};

export function useShouldShowInferDocumentationButton(entityType: EntityType) {
    return useIsDocumentationInferenceEnabled() && INFER_DOCUMENTATION_ENTITY_TYPES.includes(entityType);
}

export function useIsDocumentationInferenceEnabled() {
    const { globalSettings } = useGlobalSettingsContext();
    return globalSettings?.documentationAi?.enabled ?? false;
}
