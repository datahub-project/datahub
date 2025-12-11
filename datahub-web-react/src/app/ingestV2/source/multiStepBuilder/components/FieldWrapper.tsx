import React from 'react';
import styled from 'styled-components';

import { FieldLabel } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/components/FieldLabel';
import { HelperText } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/shared/HelperText';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

const FieldLabelWithBottomPadding = styled(FieldLabel)`
    padding-bottom: 8px;
`;

interface Props {
    label: string;
    help?: string;
    required?: boolean;
}

export function FieldWrapper({ children, label, help, required }: React.PropsWithChildren<Props>) {
    return (
        <Wrapper>
            <FieldLabelWithBottomPadding label={label} labelHelper={help} required={required} />
            {children}
            {help && <HelperText text={help} />}
        </Wrapper>
    );
}
