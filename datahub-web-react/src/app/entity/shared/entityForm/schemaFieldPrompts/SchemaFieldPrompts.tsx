import React from 'react';
import styled from 'styled-components';
import { Divider } from 'antd';
import { FormPrompt, SchemaField } from '../../../../../types.generated';
import { useGetEntityWithSchema } from '../../tabs/Dataset/Schema/useGetEntitySchema';
import SchemaFieldDropdown from './SchemaFieldDropdown';
import VirtualScrollChild from '../../../../shared/VirtualScrollChild';

const FieldPromptsTitle = styled.div`
    margin-bottom: 16px;
    font-size: 16px;
    font-weight: 600;
`;

interface Props {
    prompts: FormPrompt[];
    associatedUrn?: string;
}

export default function SchemaFieldPrompts({ prompts, associatedUrn }: Props) {
    const { entityWithSchema } = useGetEntityWithSchema();

    if (!entityWithSchema?.schemaMetadata || !entityWithSchema.schemaMetadata.fields.length) return null;

    return (
        <>
            <Divider />
            <FieldPromptsTitle data-testid="field-level-requirements">Field-Level Requirements</FieldPromptsTitle>
            {entityWithSchema?.schemaMetadata?.fields?.map((field) => (
                <VirtualScrollChild key={field.fieldPath} height={50} triggerOnce>
                    <SchemaFieldDropdown prompts={prompts} field={field as SchemaField} associatedUrn={associatedUrn} />
                </VirtualScrollChild>
            ))}
        </>
    );
}
