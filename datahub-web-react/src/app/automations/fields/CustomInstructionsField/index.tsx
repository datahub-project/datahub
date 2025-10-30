import { TextArea } from '@components';
import React from 'react';
import styled from 'styled-components';

import type { ComponentBaseProps } from '@app/automations/types';

const Container = styled.div`
    display: grid;
    gap: 12px;
`;

// State Type (ensures the state is correctly applied across templates)
export type CustomInstructionsFieldStateType = {
    customInstructions?: string;
};

// Component
export const CustomInstructionsField = ({ state, props, passStateToParent }: ComponentBaseProps) => {
    // Defined in @app/automations/fields/index
    const { placeholder, description } = props;

    // Defined in @app/automations/fields/index
    const { customInstructions } = state as CustomInstructionsFieldStateType;

    // Handle passing state to parent
    const handleChange = (value: string) => {
        passStateToParent({ customInstructions: value });
    };

    return (
        <Container>
            <TextArea
                label=""
                placeholder={placeholder}
                value={customInstructions || ''}
                onChange={(e) => handleChange(e.target.value)}
                rows={4}
            />
            {description && <span style={{ fontSize: '12px', color: '#8C8C8C' }}>{description}</span>}
        </Container>
    );
};
