import { DeleteOutlined } from '@ant-design/icons';
import { red } from '@ant-design/colors';
import { Button, Input } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY_V2 } from '../../../constants';
import { useEntityFormContext } from '../../EntityFormContext';

const StyledInput = styled(Input)`
    width: 75%;
    min-width: 350px;
    max-width: 500px;
    border: 1px solid ${ANTD_GRAY_V2[6]};
`;

const InputWrapper = styled.div`
    display: flex;
    align-items: center;
    margin-top: 8px;
`;

const StyledButton = styled(Button)<{ $displayBulkStyles?: boolean }>`
    display: block;
    margin-top: 4px;
    padding: 0;
    ${(props) => props.$displayBulkStyles && 'color: white;'}
`;

const DeleteButton = styled(Button)<{ $displayBulkStyles?: boolean }>`
    margin-left: 4px;
    ${(props) => props.$displayBulkStyles && 'color: white;'}

    &:hover {
        ${(props) => props.$displayBulkStyles && `color: ${red[3]};`}
    }
`;

interface Props {
    selectedValues: any[];
    updateSelectedValues: (values: any[]) => void;
}

export default function MultipleStringInput({ selectedValues, updateSelectedValues }: Props) {
    const { displayBulkPromptStyles } = useEntityFormContext();

    function updateInput(text: string, index: number) {
        const updatedValues =
            selectedValues.length > 0 ? selectedValues.map((value, i) => (i === index ? text : value)) : [text];
        updateSelectedValues(updatedValues);
    }

    function deleteValue(index: number) {
        const updatedValues = selectedValues.filter((_value, i) => i !== index);
        updateSelectedValues(updatedValues);
    }

    function addNewValue() {
        if (!selectedValues.length) {
            updateSelectedValues(['', '']);
        } else {
            updateSelectedValues([...selectedValues, '']);
        }
    }

    return (
        <>
            {selectedValues.length > 1 &&
                selectedValues.map((selectedValue, index) => {
                    const key = `${index}`;
                    return (
                        <InputWrapper key={key}>
                            <StyledInput
                                type="text"
                                value={selectedValue}
                                onChange={(e) => updateInput(e.target.value, index)}
                            />
                            <DeleteButton
                                $displayBulkStyles={displayBulkPromptStyles}
                                type="text"
                                icon={<DeleteOutlined />}
                                onClick={() => deleteValue(index)}
                            />
                        </InputWrapper>
                    );
                })}
            {selectedValues.length <= 1 && (
                <StyledInput
                    type="text"
                    value={selectedValues[0] || ''}
                    onChange={(e) => updateInput(e.target.value, 0)}
                />
            )}
            <StyledButton type="link" onClick={addNewValue} $displayBulkStyles={displayBulkPromptStyles}>
                + Add More
            </StyledButton>
        </>
    );
}
