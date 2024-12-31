import React from 'react';
import { SchemaField } from '../../../../../../../../types.generated';
import useBusinessAttributeRenderer from '../../utils/useBusinessAttributeRenderer';
import { SectionHeader, StyledDivider } from './components';
import SchemaEditableContext from '../../../../../../../shared/SchemaEditableContext';
import { useBusinessAttributesFlag } from '../../../../../../../useAppConfig';

interface Props {
    expandedField: SchemaField;
}

export default function FieldAttribute({ expandedField }: Props) {
    const isSchemaEditable = React.useContext(SchemaEditableContext);
    const attributeRenderer = useBusinessAttributeRenderer('', isSchemaEditable);

    const businessAttributesFlag = useBusinessAttributesFlag();

    return businessAttributesFlag ? (
        <>
            <SectionHeader>Business Attribute</SectionHeader>
            {/* pass in globalTags since this is a shared component, tags will not be shown or used */}
            <div data-testid={`schema-field-${expandedField.fieldPath}-businessAttribute`}>
                {attributeRenderer('', expandedField)}
            </div>
            <StyledDivider />
        </>
    ) : null;
}
