import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import SchemaFieldDropdown from '@app/entity/shared/entityForm/schemaFieldPrompts/SchemaFieldDropdown';
import { useGetEntityWithSchema } from '@app/entity/shared/tabs/Dataset/Schema/useGetEntitySchema';
import VirtualScrollChild from '@app/shared/VirtualScrollChild';

import { FormPrompt, SchemaField } from '@types';

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

    const schemaFields = entityWithSchema?.schemaMetadata?.fields;

    return (
        <>
            <Divider />
            <FieldPromptsTitle data-testid="field-level-requirements">Field-Level Requirements</FieldPromptsTitle>
            {schemaFields.map((field) => (
                <VirtualScrollChild key={field.fieldPath} height={50} triggerOnce>
                    <SchemaFieldDropdown
                        prompts={prompts}
                        field={field as SchemaField}
                        associatedUrn={associatedUrn}
                        schemaFields={schemaFields as SchemaField[]}
                    />
                </VirtualScrollChild>
            ))}
        </>
    );
}
