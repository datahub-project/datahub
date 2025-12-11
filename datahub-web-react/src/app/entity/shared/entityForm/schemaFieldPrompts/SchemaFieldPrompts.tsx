/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
