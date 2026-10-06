import { Button, Input } from '@components';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const MultiStringWrapper = styled.div``;

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

type Props = {
    selectedValues: (string | number | null)[];
    inputType?: string;
    updateSelectedValues: (values: (string | number | null)[]) => void;
};

export default function MultipleOpenEndedInput({ selectedValues, updateSelectedValues, inputType = 'text' }: Props) {
    const { t } = useTranslation('entityV1.shared.components');
    const { t: tc } = useTranslation('common.actions');

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
                            <Input
                                type={inputType}
                                value={selectedValue === null ? '' : String(selectedValue)}
                                setValue={(value) => updateInput(value, index)}
                            />
                            <DeleteButton
                                variant="text"
                                color="gray"
                                icon={{ icon: Trash }}
                                onClick={() => deleteValue(index)}
                                aria-label={tc('remove')}
                            />
                        </InputWrapper>
                    );
                })}
            {selectedValues.length <= 1 && (
                <Input
                    type={inputType}
                    value={selectedValues[0] === null ? '' : String(selectedValues[0] ?? '')}
                    setValue={(value) => updateInput(value, 0)}
                />
            )}
            <StyledButton variant="link" onClick={addNewValue}>
                {t('structuredProperty.addMore')}
            </StyledButton>
        </MultiStringWrapper>
    );
}
