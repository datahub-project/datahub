import React from 'react';
import { EditableSchemaMetadata, GlobalTags, SchemaField } from '../../../../../../../../types.generated';
import useTagsAndTermsRenderer from '../../utils/useTagsAndTermsRenderer';
import { SectionHeader, StyledDivider } from './components';
import SchemaEditableContext from '../../../../../../../shared/SchemaEditableContext';

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
            <SectionHeader>Tags</SectionHeader>
            <div data-testid={`schema-field-${expandedField.fieldPath}-tags`}>
                {tagRenderer(expandedField.globalTags as GlobalTags, expandedField)}
            </div>
            <StyledDivider />
        </>
    );
}
