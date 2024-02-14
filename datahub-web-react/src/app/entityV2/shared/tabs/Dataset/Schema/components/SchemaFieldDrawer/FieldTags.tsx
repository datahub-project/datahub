import React from 'react';
import styled from 'styled-components';
import { EditableSchemaMetadata, GlobalTags, SchemaField } from '../../../../../../../../types.generated';
import useTagsAndTermsRenderer from '../../utils/useTagsAndTermsRenderer';
import { SectionHeader, StyledDivider } from './components';
import SchemaEditableContext from '../../../../../../../shared/SchemaEditableContext';

const TagsWrapper = styled.div`
    margin-bottom: 24px;
`;

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
        <TagsWrapper>
            <SectionHeader>Tags</SectionHeader>
            {tagRenderer(expandedField.globalTags as GlobalTags, expandedField)}
            <StyledDivider dashed />
        </TagsWrapper>
    );
}
