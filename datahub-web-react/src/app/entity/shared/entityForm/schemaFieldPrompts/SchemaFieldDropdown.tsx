/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Collapse } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import Prompt from '@app/entity/shared/entityForm/prompts/Prompt';
import DropdownHeader from '@app/entity/shared/entityForm/schemaFieldPrompts/DropdownHeader';

import { FormPrompt, SchemaField } from '@types';

const StyledCollapse = styled(Collapse)`
    margin-bottom: 16px;

    .ant-collapse-header {
        font-size: 14px;
        font-weight: bold;
        padding: 12px 0;
    }
    &&& .ant-collapse-item {
        background-color: white;
        border-radius: 5px;
    }
    .ant-collapse-content-box {
        padding: 0;
    }
`;

interface Props {
    field: SchemaField;
    prompts: FormPrompt[];
    associatedUrn?: string;
}

export default function SchemaFieldDropdown({ field, prompts, associatedUrn }: Props) {
    const [isExpanded, setIsExpanded] = useState(false);
    return (
        <StyledCollapse onChange={() => setIsExpanded(!isExpanded)}>
            <Collapse.Panel
                header={<DropdownHeader field={field} numPrompts={prompts.length} isExpanded={isExpanded} />}
                key="0"
            >
                {prompts.map((prompt) => (
                    <Prompt key={prompt.id} prompt={prompt as FormPrompt} field={field} associatedUrn={associatedUrn} />
                ))}
            </Collapse.Panel>
        </StyledCollapse>
    );
}
