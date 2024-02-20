import React from 'react';

import styled from 'styled-components';
import { Collapse, Input, Typography } from 'antd';
import TextArea from 'antd/lib/input/TextArea';

import { AssertionMonitorBuilderState } from '../../types';
import { updateExecutorIdState } from '../utils';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
`;

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (newState: AssertionMonitorBuilderState) => void;
};

/**
 * Final step in assertion creation flow: Give it a name / description.
 */
export const FinishUpBuilder = ({ state, updateState }: Props) => {
    const description = state.assertion?.description;

    const updateDescription = (newDescription: string) => {
        const finalDescription = newDescription || null;
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                description: finalDescription,
            },
        });
    };

    return (
        <div>
            <Section>
                <Typography.Title level={5}>Name</Typography.Title>
                <TextArea
                    value={description || ''}
                    placeholder="Give this assertion a name (optional)"
                    onChange={(e) => updateDescription(e.target.value)}
                />
                <Typography.Paragraph style={{ marginTop: 4 }} type="secondary">
                    If not specified, a name will be generated from the assertion settings.
                </Typography.Paragraph>
            </Section>
            <Section>
                <Collapse>
                    <Collapse.Panel key="Advanced" header="Advanced">
                        <Typography.Title level={5}>Executor Id (Optional)</Typography.Title>
                        <Typography.Paragraph type="secondary">
                            Configure monitoring using a remote executor by providing a custom executor id. You should
                            only change this field if a remote executor has been configured.
                        </Typography.Paragraph>
                        <Input
                            value={state.executorId || ''}
                            onChange={(e) => updateState(updateExecutorIdState(state, e.target.value))}
                            placeholder="default"
                        />
                    </Collapse.Panel>
                </Collapse>
            </Section>
        </div>
    );
};
