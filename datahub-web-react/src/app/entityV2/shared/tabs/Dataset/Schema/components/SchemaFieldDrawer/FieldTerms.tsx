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

export default function FieldTerms({ expandedField, editableSchemaMetadata }: Props) {
    const isSchemaEditable = React.useContext(SchemaEditableContext);
    const termRenderer = useTagsAndTermsRenderer(
        editableSchemaMetadata,
        {
            showTags: false,
            showTerms: true,
        },
        '',
        isSchemaEditable,
    );

    return (
        <>
            <SidebarSection
                title="Glossary Terms"
                content={termRenderer(expandedField.globalTags as GlobalTags, expandedField)}
            />
            <StyledDivider dashed />
        </>
    );
}
