import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { DocumentationAssociation } from '@src/types.generated';

function checkIsInferredDocumentation(documentation?: DocumentationAssociation) {
    return !!documentation?.attribution?.sourceDetail?.find(
        (mapEntry) => mapEntry.key === 'inferred' && mapEntry.value === 'true',
    );
}

export function getAssetDescriptionDetails({
    entityProperties,
    defaultDescription,
    enableInferredDescriptions,
}: {
    entityProperties?: GenericEntityProperties | null;
    defaultDescription?: string | null;
    enableInferredDescriptions?: boolean;
}) {
    // get most recent documentation aspect
    const sortedDocumentations = entityProperties?.documentation?.documentations
        ?.filter((documentation) => enableInferredDescriptions || !checkIsInferredDocumentation(documentation))
        .sort((doc1, doc2) => (doc2.attribution?.time || 0) - (doc1.attribution?.time || 0));
    const documentation = sortedDocumentations?.[0];

    // Check user set documentation
    const editedDescription = entityProperties?.editableProperties?.description;
    const originalDescription = entityProperties?.properties?.description;
    const editableDescription = editedDescription || originalDescription || '';
    const isUsingDocumentationAspect = !editableDescription && !!documentation;

    const isInferred = isUsingDocumentationAspect && checkIsInferredDocumentation(documentation);

    // Show the inferred documentation only when the editedDescription is undefined and orginal description is empty
    const displayedDescription =
        editedDescription ?? (originalDescription || documentation?.documentation || defaultDescription || '');

    const sourceDetail = documentation?.attribution?.sourceDetail;
    const inferredDescription = isInferred ? documentation.documentation : undefined;

    return {
        displayedDescription,
        isUsingDocumentationAspect,
        isInferred,
        sourceDetail,
        inferredDescription,
    };
}
