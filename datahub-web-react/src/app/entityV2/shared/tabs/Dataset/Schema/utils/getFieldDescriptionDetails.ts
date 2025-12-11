/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { EditableSchemaFieldInfo, SchemaFieldEntity } from '@types';

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
    const attribution = documentation?.attribution;
    const isPropagated =
        isUsingDocumentationAspect &&
        !!attribution?.sourceDetail?.find((mapEntry) => mapEntry.key === 'propagated' && mapEntry.value === 'true');

    const displayedDescription =
        editableFieldInfo?.description ?? (defaultDescription || documentation?.documentation || '');

    const sourceDetail = attribution?.sourceDetail;
    const propagatedDescription = isPropagated ? documentation?.documentation : undefined;

    return {
        displayedDescription,
        isPropagated,
        sourceDetail,
        propagatedDescription,
        attribution,
    };
}
