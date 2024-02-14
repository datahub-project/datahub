import React, { useState } from 'react';

import styled from 'styled-components';
import { Button } from 'antd';

import { StepProps } from '../types';
import { FinishUpBuilder } from './finish/FinishUpBuilder';

const Step = styled.div`
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
`;

const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

/**
 * Final step in assertion creation flow: Give it a name / description.
 */
export const FinishUpStep = ({ state, updateState, prev, submit }: StepProps) => {
    const [isSubmitting, setSubmitting] = useState(false);
    return (
        <Step>
            <FinishUpBuilder state={state} updateState={updateState} />
            <ControlsContainer>
                <Button onClick={prev}>Back</Button>
                <Button
                    type="primary"
                    onClick={async () => {
                        try {
                            setSubmitting(true);
                            await submit();
                        } finally {
                            setSubmitting(false);
                        }
                    }}
                    disabled={isSubmitting}
                >
                    Save
                </Button>
            </ControlsContainer>
        </Step>
    );
};
