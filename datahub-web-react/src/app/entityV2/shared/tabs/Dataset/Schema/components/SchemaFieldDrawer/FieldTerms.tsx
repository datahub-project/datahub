import React from 'react';
import { EditableSchemaMetadata, GlobalTags, SchemaField } from '../../../../../../../../types.generated';
import SchemaEditableContext from '../../../../../../../shared/SchemaEditableContext';
import { SidebarSection } from '../../../../../containers/profile/sidebar/SidebarSection';
import useTagsAndTermsRenderer from '../../utils/useTagsAndTermsRenderer';
import { StyledDivider } from './components';

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
