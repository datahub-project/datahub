import React from 'react';
import { Checkbox } from 'antd'; // Adjust imports as necessary
import styled from 'styled-components';
import { Tag as TagType, TagAssociation } from '@src/types.generated';
import Tag from '@src/app/sharedV2/tags/tag/Tag';

// Styled components
const StyledCheckboxLabel = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    width: inherit;
    padding: 0px 6px 0px 4px;
    cursor: pointer;
`;

const StyledCheckboxGroup = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    margin-bottom: 12px;
`;

const StyledCheckbox = styled(Checkbox)`
    margin-left: 4px;
    margin-bottom: 4px;
`;
// Define the props for the new component
interface CheckboxWithTagsProps {
    options: Array<{ value: string; tag: TagType }>;
    handleCheckboxToggle: (value: string) => void;
}

export const AcrylTagCheckboxGroup: React.FC<CheckboxWithTagsProps> = ({ options, handleCheckboxToggle }) => {
    return (
        <StyledCheckboxGroup>
            {options.map((option) => (
                <StyledCheckboxLabel key={option.value} onClick={() => handleCheckboxToggle(option.value)}>
                    <Tag
                        tag={{ tag: option.tag } as TagAssociation}
                        options={{ shouldNotOpenDrawerOnClick: true }}
                        maxWidth={120}
                    />
                    <StyledCheckbox value={option.value} onChange={() => handleCheckboxToggle(option.value)} />
                </StyledCheckboxLabel>
            ))}
        </StyledCheckboxGroup>
    );
};
