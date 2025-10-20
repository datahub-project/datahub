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

    const attribution = documentation?.attribution;
    const isPropagated =
        isUsingDocumentationAspect &&
        !!attribution?.sourceDetail?.find((mapEntry) => mapEntry.key === 'propagated' && mapEntry.value === 'true');
    const isInferred = isUsingDocumentationAspect && checkIsInferredDocumentation(documentation);

    const sourceDetail = attribution?.sourceDetail;
    const propagatedDescription = isPropagated ? documentation?.documentation : undefined;
    const inferredDescription = isInferred ? documentation.documentation : undefined;

    // Priority:
    // 1. EditableSchemaMetadata description (manually entered in UI)
    // 2. SchemaMetadata description (ingested)
    // 3. Propagated description
    // 4. Empty EditableSchemaMetadata description (to hide inferred description)
    // 5. Inferred description (from AI): only shown when the editedDescription is undefined and original description is empty
    // 6. Default description
    const displayedDescription =
        editedDescription ||
        originalDescription ||
        propagatedDescription ||
        (editedDescription ?? (documentation?.documentation || defaultDescription || ''));

    return {
        displayedDescription,
        isUsingDocumentationAspect,
        isPropagated,
        isInferred,
        sourceDetail,
        propagatedDescription,
        inferredDescription,
        attribution,
    };
}
