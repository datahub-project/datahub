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
