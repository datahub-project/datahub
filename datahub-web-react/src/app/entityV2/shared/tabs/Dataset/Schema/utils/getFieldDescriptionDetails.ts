import {
    DocumentationAssociation,
    EditableSchemaFieldInfo,
    SchemaFieldEntity,
} from '../../../../../../../types.generated';

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

export function getFieldDescriptionDetails({
    schemaFieldEntity,
    editableFieldInfo,
    defaultDescription,
    enableInferredDescriptions,
}: Props) {
    // get most recent documentation
    const sortedDocumentations = schemaFieldEntity?.documentation?.documentations
        ?.filter((documentation) => enableInferredDescriptions || !checkIsInferredDocumentation(documentation))
        ?.sort((doc1, doc2) => (doc2.attribution?.time || 0) - (doc1.attribution?.time || 0));
    const documentation = sortedDocumentations?.[0];
    const isUsingDocumentationAspect = !editableFieldInfo?.description && !defaultDescription && !!documentation;
    const isPropagated =
        isUsingDocumentationAspect &&
        !!documentation?.attribution?.sourceDetail?.find(
            (mapEntry) => mapEntry.key === 'propagated' && mapEntry.value === 'true',
        );
    const isInferred = isUsingDocumentationAspect && checkIsInferredDocumentation(documentation);

    const displayedDescription =
        editableFieldInfo?.description || defaultDescription || documentation?.documentation || '';

    const sourceDetail = documentation?.attribution?.sourceDetail;
    const propagatedDescription = isPropagated ? documentation?.documentation : undefined;
    const inferredDescription = isInferred ? documentation.documentation : undefined;

    return { displayedDescription, isPropagated, isInferred, sourceDetail, propagatedDescription, inferredDescription };
}
