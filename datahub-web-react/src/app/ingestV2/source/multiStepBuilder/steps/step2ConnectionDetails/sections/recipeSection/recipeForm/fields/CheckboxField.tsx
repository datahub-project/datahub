import { NativeCheckbox, Text } from '@components';
import styled from 'styled-components';

import { RecipeFormItem } from './RecipeFormItem';
import { CommonFieldProps } from './types';

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

export function CheckboxField({ field }: CommonFieldProps) {
    return (
        <RecipeFormItem
            recipeField={field}
            style={{ flexDirection: 'row', alignItems: 'center' }}
            valuePropName={'checked'}
        >
            <CheckboxWithHelper>
                <NativeCheckbox justifyContent="flex-start" />
                <Text size="sm" color="gray" colorLevel={600}>
                    {field.helper}
                </Text>
            </CheckboxWithHelper>
        </RecipeFormItem>
    );
}
