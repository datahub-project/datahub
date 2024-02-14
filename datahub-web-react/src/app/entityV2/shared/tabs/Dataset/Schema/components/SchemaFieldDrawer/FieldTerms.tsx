import React from 'react';
import styled from 'styled-components';
import { EditableSchemaMetadata, GlobalTags, SchemaField } from '../../../../../../../../types.generated';
import useTagsAndTermsRenderer from '../../utils/useTagsAndTermsRenderer';
import { SectionHeader, StyledDivider } from './components';
import SchemaEditableContext from '../../../../../../../shared/SchemaEditableContext';

const TermsWrapper = styled.div`
    margin-bottom: 24px;
`;

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
        <TermsWrapper>
            <SectionHeader>Glossary Terms</SectionHeader>
            {/* pass in globalTags since this is a shared component, tags will not be shown or used */}
            {termRenderer(expandedField.globalTags as GlobalTags, expandedField)}
            <StyledDivider dashed />
        </TermsWrapper>
    );
}
