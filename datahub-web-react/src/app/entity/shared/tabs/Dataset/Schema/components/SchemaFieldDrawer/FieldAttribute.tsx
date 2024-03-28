import React from 'react';
import { EditableSchemaMetadata, SchemaField } from '../../../../../../../../types.generated';
import useBusinessAttributeRenderer from '../../utils/useBusinessAttributeRenderer';
import { SectionHeader, StyledDivider } from './components';
import SchemaEditableContext from '../../../../../../../shared/SchemaEditableContext';

interface Props {
    expandedField: SchemaField;
    editableSchemaMetadata?: EditableSchemaMetadata | null;
}

export default function FieldAttribute({ expandedField, editableSchemaMetadata }: Props) {
    const isSchemaEditable = React.useContext(SchemaEditableContext);
    const attributeRenderer = useBusinessAttributeRenderer(
        editableSchemaMetadata,
        '',
        isSchemaEditable,
    );

    return (
        <>
            <SectionHeader>Business Attribute</SectionHeader>
            {/* pass in globalTags since this is a shared component, tags will not be shown or used */}
            <div data-testid={`schema-field-${expandedField.fieldPath}-businessAttribute`}>
                {attributeRenderer(editableSchemaMetadata, expandedField)}
            </div>
            <StyledDivider />
        </>
    );
}
