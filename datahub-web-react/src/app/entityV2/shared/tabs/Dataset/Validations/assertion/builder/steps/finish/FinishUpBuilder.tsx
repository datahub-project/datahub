import React from 'react';

import styled from 'styled-components';
import { Typography } from 'antd';
import TextArea from 'antd/lib/input/TextArea';

import { AssertionMonitorBuilderState } from '../../types';
import { AssertionType } from '../../../../../../../../../../types.generated';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
`;

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (newState: AssertionMonitorBuilderState) => void;
    onTitleChange?: (title: string) => void;
};

/**
 * Final step in assertion creation flow: Give it a name / description.
 */
export const FinishUpBuilder = ({ state, updateState, onTitleChange }: Props) => {
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
        onTitleChange?.(newDescription);
    };

    return (
        <div>
            <Section>
                <Typography.Title level={5}>Name</Typography.Title>
                <TextArea
                    value={description || ''}
                    placeholder={`Give this assertion a name${state.assertion?.type === AssertionType.Sql ? '' : ' (optional)'}`}
                    onChange={(e) => updateDescription(e.target.value)}
                />
                <Typography.Paragraph style={{ marginTop: 4 }} type="secondary">
                    {state.assertion?.type === AssertionType.Sql ? 'This is required for SQL assertions.' : 'If not specified, a name will be generated from the assertion settings.'}
                </Typography.Paragraph>
            </Section>
        </div>
    );
};
