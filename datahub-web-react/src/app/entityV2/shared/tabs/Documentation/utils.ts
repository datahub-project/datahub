import { DocumentationAssociation } from '@types';
import { GenericEntityProperties } from '@src/app/entity/shared/types';

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

export function getAssetDescriptionDetails({
    entityProperties,
    defaultDescription,
    enableInferredDescriptions,
}: {
    entityProperties?: GenericEntityProperties | null;
    defaultDescription?: string | null;
    enableInferredDescriptions?: boolean;
}) {
    // Filter and sort documentations
    const filteredDocumentations = entityProperties?.documentation?.documentations?.filter(
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

    // Check user set documentation from other aspects
    const editedDescription = entityProperties?.editableProperties?.description;
    const originalDescription = entityProperties?.properties?.description;
    const editableDescription = editedDescription || originalDescription || '';
    const isUsingDocumentationAspect = !editableDescription && !!documentation;

    const attribution = documentation?.attribution;
    const isPropagated = isUsingDocumentationAspect && checkIsPropagatedDocumentation(documentation);
    const isInferred = isUsingDocumentationAspect && checkIsInferredDocumentation(documentation);
    const isUiAuthored = isUsingDocumentationAspect && checkIsUiAuthoredDocumentation(documentation);

    const sourceDetail = attribution?.sourceDetail;
    const uiAuthoredDescription = uiAuthoredDoc?.documentation;
    const propagatedDescription = propagatedDoc?.documentation;
    const inferredDescription = otherDoc && checkIsInferredDocumentation(otherDoc) ? otherDoc.documentation : undefined;

    // Priority:
    // 1. EditableProperties description (manually entered in UI for entities with this aspect)
    // 2. Properties description (ingested)
    // 3. UI-authored documentation (from Documentation aspect with source=UI)
    // 4. Propagated documentation (from Documentation aspect with propagated=true)
    // 5. Other documentation (inferred, ingested via Documentation aspect)
    // 6. Default description
    const displayedDescription =
        editedDescription ||
        originalDescription ||
        uiAuthoredDescription ||
        propagatedDescription ||
        documentation?.documentation ||
        defaultDescription ||
        '';

    return {
        displayedDescription,
        isUsingDocumentationAspect,
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
