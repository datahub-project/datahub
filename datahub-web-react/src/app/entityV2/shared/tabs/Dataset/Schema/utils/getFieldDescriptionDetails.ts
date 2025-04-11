import { EditableSchemaFieldInfo, SchemaFieldEntity } from '../../../../../../../types.generated';

interface Props {
    schemaFieldEntity?: SchemaFieldEntity | null;
    editableFieldInfo?: EditableSchemaFieldInfo;
    defaultDescription?: string | null;
}

export function getFieldDescriptionDetails({ schemaFieldEntity, editableFieldInfo, defaultDescription }: Props) {
    // get most recent documentation
    const sortedDocumentations = schemaFieldEntity?.documentation?.documentations?.sort(
        (doc1, doc2) => (doc2.attribution?.time || 0) - (doc1.attribution?.time || 0),
    );
    const documentation = sortedDocumentations?.[0];
    const isUsingDocumentationAspect = !editableFieldInfo?.description && !defaultDescription && !!documentation;
    const isPropagated =
        isUsingDocumentationAspect &&
        !!documentation?.attribution?.sourceDetail?.find(
            (mapEntry) => mapEntry.key === 'propagated' && mapEntry.value === 'true',
        );

    const displayedDescription =
        editableFieldInfo?.description || defaultDescription || documentation?.documentation || '';

    const sourceDetail = documentation?.attribution?.sourceDetail;
    const propagatedDescription = isPropagated ? documentation?.documentation : undefined;

    return { displayedDescription, isPropagated, sourceDetail, propagatedDescription };
}
