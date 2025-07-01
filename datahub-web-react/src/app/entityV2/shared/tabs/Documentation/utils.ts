import { GenericEntityProperties } from '@src/app/entity/shared/types';

export function getAssetDescriptionDetails({
    entityProperties,
    defaultDescription,
}: {
    entityProperties?: GenericEntityProperties | null;
    defaultDescription?: string | null;
}) {
    // get most recent documentation aspect
    const sortedDocumentations = entityProperties?.documentation?.documentations?.sort(
        (doc1, doc2) => (doc2.attribution?.time || 0) - (doc1.attribution?.time || 0),
    );
    const documentation = sortedDocumentations?.[0];

    // Check user set documentation
    const editedDescription = entityProperties?.editableProperties?.description;
    const originalDescription = entityProperties?.properties?.description;
    const editableDescription = editedDescription || originalDescription || '';
    const isUsingDocumentationAspect = !editableDescription && !!documentation;

    const displayedDescription = editableDescription || documentation?.documentation || defaultDescription || '';

    const sourceDetail = documentation?.attribution?.sourceDetail;

    return {
        displayedDescription,
        isUsingDocumentationAspect,
        sourceDetail,
    };
}
