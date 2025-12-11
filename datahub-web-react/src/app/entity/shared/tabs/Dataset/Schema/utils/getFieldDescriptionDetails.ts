/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DocumentationAssociation, EditableSchemaFieldInfo, SchemaFieldEntity } from '@types';

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
