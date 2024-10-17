import {
    EditableSchemaFieldInfo,
    SchemaFieldEntity,
    DocumentationAssociation,
} from '../../../../../../../types.generated';

interface Props {
    schemaFieldEntity?: SchemaFieldEntity | null;
    editableFieldInfo?: EditableSchemaFieldInfo;
    defaultDescription?: string | null;
}

export function getFieldDescriptionDetails({ schemaFieldEntity, editableFieldInfo, defaultDescription }: Props) {
    const documentations = schemaFieldEntity?.documentation?.documentations;
    let latestDocumentation: DocumentationAssociation | undefined;

    if (documentations && documentations.length > 0) {
        latestDocumentation = documentations.reduce((latest, current) => {
            const latestTime = latest.attribution?.time || 0;
            const currentTime = current.attribution?.time || 0;
            return currentTime > latestTime ? current : latest;
        });
    }

    const isUsingDocumentationAspect = !editableFieldInfo?.description && !!latestDocumentation;
    const isPropagated =
        isUsingDocumentationAspect &&
        !!latestDocumentation?.attribution?.sourceDetail?.find(
            (mapEntry) => mapEntry.key === 'propagated' && mapEntry.value === 'true',
        );

    const displayedDescription =
        editableFieldInfo?.description || latestDocumentation?.documentation || defaultDescription || '';

    const sourceDetail = latestDocumentation?.attribution?.sourceDetail;
    const propagatedDescription = latestDocumentation?.documentation;

    return { displayedDescription, isPropagated, sourceDetail, propagatedDescription };
}
