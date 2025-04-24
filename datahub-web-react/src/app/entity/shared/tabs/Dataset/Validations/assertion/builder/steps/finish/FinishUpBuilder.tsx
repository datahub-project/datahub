import { Typography } from 'antd';
import TextArea from 'antd/lib/input/TextArea';
import React, { useEffect } from 'react';
import styled from 'styled-components';

import { AssertionMonitorBuilderState } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/types';

import { AssertionType } from '@types';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
`;

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (newState: AssertionMonitorBuilderState) => void;
    onValidityChange?: (isValid: boolean) => void;
};

/**
 * Final step in assertion creation flow: Give it a name / description.
 */
export const FinishUpBuilder = ({ state, updateState, onValidityChange }: Props) => {
    const description = state.assertion?.description;
    const isDescriptionRequired = state.assertion?.type === AssertionType.Sql;
    useEffect(() => {
        onValidityChange?.(!isDescriptionRequired || !!description?.length);
    }, [description, isDescriptionRequired, onValidityChange]);

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
                    placeholder={`Give this assertion a name${isDescriptionRequired ? '' : ' (optional)'}`}
                    onChange={(e) => updateDescription(e.target.value)}
                />
                <Typography.Paragraph style={{ marginTop: 4 }} type="secondary">
                    {isDescriptionRequired
                        ? 'Required for this assertion type.'
                        : 'If not specified, a name will be generated from the assertion settings.'}
                </Typography.Paragraph>
            </Section>
        </div>
    );
};
