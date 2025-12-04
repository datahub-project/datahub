import { Checkbox, NativeCheckbox, Text } from '@components';
import styled from 'styled-components';

import { Field } from '@app/ingestV2/source/multiStepBuilder/components/Field';

import { RecipeFormItem } from './RecipeFormItem';
import { CommonFieldProps } from './types';
import { useCallback, useMemo } from 'react';

const CheckboxWithHelper = styled.div`
    // compensate checkbox container size
    // see datahub-web-react/src/alchemy-components/components/Checkbox/components.ts -> CheckboxBase for details
    position: relative;
    left: -5px;

    display: flex;
    flex-direction: row;
    gap: 4px;
    align-items: center;
`;

interface Props {
    checked?: boolean;
    onChange?: (newValue: boolean) => void;

    helper?: string;
}

function AntdFormCompatibmeCheckbox({ checked, onChange, helper }: Props) {
    console.log('>>>AntdFormCompatibmeCheckbox', {
        checked, onChange
    });

    return (
        <CheckboxWithHelper>
            <Checkbox isChecked={checked} onCheckboxChange={onChange} justifyContent="flex-start" />
            {helper && (
                <Text size="sm" color="gray" colorLevel={600}>
                    {helper}
                </Text>
            )}
        </CheckboxWithHelper>
    );
}

export function CheckboxField({ field }: CommonFieldProps) {
    return (
        <RecipeFormItem
            recipeField={field}
            style={{ flexDirection: 'row', alignItems: 'center' }}
            valuePropName={'checked'}
        >
            <AntdFormCompatibmeCheckbox helper={field.helper} />
        </RecipeFormItem>
    );
}
