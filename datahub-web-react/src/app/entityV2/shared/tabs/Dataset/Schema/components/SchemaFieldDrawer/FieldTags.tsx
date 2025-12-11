/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { StyledDivider } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import useTagsAndTermsRenderer from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useTagsAndTermsRenderer';
import SchemaEditableContext from '@app/shared/SchemaEditableContext';

import { EditableSchemaMetadata, GlobalTags, SchemaField } from '@types';

interface Props {
    expandedField: SchemaField;
    editableSchemaMetadata?: EditableSchemaMetadata | null;
}

export default function FieldTags({ expandedField, editableSchemaMetadata }: Props) {
    const isSchemaEditable = React.useContext(SchemaEditableContext);
    const tagRenderer = useTagsAndTermsRenderer(
        editableSchemaMetadata,
        {
            showTags: true,
            showTerms: false,
        },
        '',
        isSchemaEditable,
    );

    return (
        <>
            <SidebarSection title="Tags" content={tagRenderer(expandedField.globalTags as GlobalTags, expandedField)} />
            <StyledDivider dashed />
        </>
    );
}
