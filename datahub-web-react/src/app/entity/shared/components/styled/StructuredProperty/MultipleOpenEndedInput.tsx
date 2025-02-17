import { DeleteOutlined } from '@ant-design/icons';
import { Button, Input } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY_V2 } from '../../../constants';

const MultiStringWrapper = styled.div``;

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

const StyledButton = styled(Button)`
    display: block;
    margin-top: 4px;
    padding: 0;
`;

const DeleteButton = styled(Button)`
    margin-left: 4px;
`;

interface Props {
    selectedValues: any[];
    inputType?: string;
    updateSelectedValues: (values: any[]) => void;
}

export default function MultipleOpenEndedInput({ selectedValues, updateSelectedValues, inputType = 'text' }: Props) {
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
        <MultiStringWrapper>
            {selectedValues.length > 1 &&
                selectedValues.map((selectedValue, index) => {
                    const key = `${index}`;
                    return (
                        <InputWrapper key={key}>
                            <StyledInput
                                type={inputType}
                                value={selectedValue}
                                onChange={(e) => updateInput(e.target.value, index)}
                            />
                            <DeleteButton type="text" icon={<DeleteOutlined />} onClick={() => deleteValue(index)} />
                        </InputWrapper>
                    );
                })}
            {selectedValues.length <= 1 && (
                <StyledInput
                    type={inputType}
                    value={selectedValues[0] || ''}
                    onChange={(e) => updateInput(e.target.value, 0)}
                />
            )}
            <StyledButton type="link" onClick={addNewValue}>
                + Add More
            </StyledButton>
        </MultiStringWrapper>
    );
}
