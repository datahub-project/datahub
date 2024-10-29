import React from 'react';
import { EditableSchemaMetadata, GlobalTags, SchemaField } from '../../../../../../../../types.generated';
import useTagsAndTermsRenderer from '../../utils/useTagsAndTermsRenderer';
import { SectionHeader, StyledDivider } from './components';
import SchemaEditableContext from '../../../../../../../shared/SchemaEditableContext';

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
            <SectionHeader>Glossary Terms</SectionHeader>
            {/* pass in globalTags since this is a shared component, tags will not be shown or used */}
            <div data-testid={`schema-field-${expandedField.fieldPath}-terms`}>
                {termRenderer(expandedField.globalTags as GlobalTags, expandedField)}
            </div>
            <StyledDivider />
        </>
    );
}
