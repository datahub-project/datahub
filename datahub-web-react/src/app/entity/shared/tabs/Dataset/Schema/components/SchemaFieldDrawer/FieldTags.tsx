import React from 'react';

import {
    SectionHeader,
    StyledDivider,
} from '@app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import useTagsAndTermsRenderer from '@app/entity/shared/tabs/Dataset/Schema/utils/useTagsAndTermsRenderer';
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
            <SectionHeader>Tags</SectionHeader>
            <div data-testid={`schema-field-${expandedField.fieldPath}-tags`}>
                {tagRenderer(expandedField.globalTags as GlobalTags, expandedField)}
            </div>
            <StyledDivider />
        </>
    );
}
