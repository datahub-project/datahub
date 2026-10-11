import { DocumentationAssociation, EditableSchemaFieldInfo, SchemaFieldEntity } from '@types';

interface Props {
    schemaFieldEntity?: SchemaFieldEntity | null;
    editableFieldInfo?: EditableSchemaFieldInfo;
    defaultDescription?: string | null;
    enableInferredDescriptions?: boolean;
}

function checkIsInferredDocumentation(documentation?: DocumentationAssociation) {
    return !!documentation?.attribution?.sourceDetail?.find(
        (mapEntry) => mapEntry.key === 'inferred' && mapEntry.value === 'true',
    );
}

function checkIsPropagatedDocumentation(documentation?: DocumentationAssociation) {
    return !!documentation?.attribution?.sourceDetail?.find(
        (mapEntry) => mapEntry.key === 'propagated' && mapEntry.value === 'true',
    );
}

/**
 * Checks if documentation was authored via the UI.
 * UI-authored documentation has sourceDetail with "ui" = "true".
 */
function checkIsUiAuthoredDocumentation(documentation?: DocumentationAssociation) {
    return !!documentation?.attribution?.sourceDetail?.find(
        (mapEntry) => mapEntry.key === 'ui' && mapEntry.value === 'true',
    );
}

export function getFieldDescriptionDetails({
    schemaFieldEntity,
    editableFieldInfo,
    defaultDescription,
    enableInferredDescriptions,
}: Props) {
    // Filter documentations
    const filteredDocumentations = schemaFieldEntity?.documentation?.documentations?.filter(
        (documentation) => enableInferredDescriptions || !checkIsInferredDocumentation(documentation),
    );

    // Find UI-authored documentation (highest priority in the Documentation aspect)
    const uiAuthoredDoc = filteredDocumentations
        ?.filter((doc) => checkIsUiAuthoredDocumentation(doc))
        .sort((doc1, doc2) => (doc2.attribution?.time || 0) - (doc1.attribution?.time || 0))?.[0];

    // Find propagated documentation
    const propagatedDoc = filteredDocumentations
        ?.filter((doc) => checkIsPropagatedDocumentation(doc))
        .sort((doc1, doc2) => (doc2.attribution?.time || 0) - (doc1.attribution?.time || 0))?.[0];

    // Get most recent non-UI, non-propagated documentation (could be inferred or ingested)
    const otherDoc = filteredDocumentations
        ?.filter((doc) => !checkIsUiAuthoredDocumentation(doc) && !checkIsPropagatedDocumentation(doc))
        .sort((doc1, doc2) => (doc2.attribution?.time || 0) - (doc1.attribution?.time || 0))?.[0];

    // Priority within Documentation aspect: UI-authored > Propagated > Other (inferred/ingested)
    const documentation = uiAuthoredDoc || propagatedDoc || otherDoc;

    const isUsingDocumentationAspect = !editableFieldInfo?.description && !defaultDescription && !!documentation;
    const attribution = documentation?.attribution;
    const isPropagated = isUsingDocumentationAspect && checkIsPropagatedDocumentation(documentation);
    const isInferred = isUsingDocumentationAspect && checkIsInferredDocumentation(documentation);
    const isUiAuthored = isUsingDocumentationAspect && checkIsUiAuthoredDocumentation(documentation);

    const uiAuthoredDescription = uiAuthoredDoc?.documentation;
    const propagatedDescription = propagatedDoc?.documentation;
    // Only set inferredDescription when not using editableFieldInfo
    const inferredDescription =
        !editableFieldInfo?.description && otherDoc && checkIsInferredDocumentation(otherDoc)
            ? otherDoc.documentation
            : undefined;

    const displayedDescription =
        editableFieldInfo?.description ??
        (defaultDescription || uiAuthoredDescription || propagatedDescription || documentation?.documentation || '');

    const sourceDetail = attribution?.sourceDetail;

    return {
        displayedDescription,
        isPropagated,
        isInferred,
        isUiAuthored,
        sourceDetail,
        uiAuthoredDescription,
        propagatedDescription,
        inferredDescription,
        attribution,
    };
}
