
import React from 'react';
import styled from 'styled-components';
import { Button, Tooltip } from 'antd';
import { AssertionBuilderStep, StepProps } from '../types';
import { AssertionActionsSection } from './actions/AssertionActionsSection';
import { SchemaAssertionBuilder } from './schema/SchemaAssertionBuilder';

const Step = styled.div`
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
`;

const Controls = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
    margin-bottom: 20px;
`;

const ControlsGroup = styled.div`
    display: flex;
    gap: 8px;
`;

/**
 * Step for defining the Dataset Schema assertion
 * TODO: Add support for trying this type of assertion out. 
 */
export const ConfigureDatasetSchemaAssertionStep = ({ state, updateState, goTo, prev }: StepProps) => {
    return (
        <Step>
            <div>
                <SchemaAssertionBuilder state={state} updateState={updateState} disabled={false} />
                <AssertionActionsSection state={state} updateState={updateState} />
            </div>
            <Controls>
                <Button onClick={prev}>Back</Button>
                <ControlsGroup>
                    <Tooltip
                        title="Coming soon!"
                    >
                        <Button onClick={undefined} disabled>Try it out</Button>
                    </Tooltip>
                    <Button type="primary" onClick={() => goTo(AssertionBuilderStep.FINISH_UP)}>
                        Next
                    </Button>
                </ControlsGroup>
            </Controls>
        </Step>
    );
};
