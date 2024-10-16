import React, { useState } from 'react';
import styled from 'styled-components';
import { Collapse } from 'antd';
import { FormPrompt, SchemaField } from '../../../../../types.generated';
import Prompt from '../prompts/Prompt';
import DropdownHeader from './DropdownHeader';

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
