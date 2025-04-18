import React from 'react';
import styled from 'styled-components';
import { Timeline, Typography } from 'antd';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { INSTRUCTIONS_TITLE, STEPS } from './constants';

const Container = styled.div`
    margin: 4px;
    padding: 28px 28px 28px 28px;
    box-shadow: 0px 0px 6px 0px #e8e8e8;
`;

const InstructionsTitleContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
    margin-bottom: 28px;
`;

const InstructionsTitle = styled(Typography.Title)`
    && {
        color: ${ANTD_GRAY[8]};
        margin: 0px;
        padding: 0px;
    }
`;

const StepDescription = styled.div`
    color: ${ANTD_GRAY[8]};
    margin-bottom: 12px;
`;

export const Instructions = () => {
    return (
        <Container>
            <InstructionsTitleContainer>
                <InstructionsTitle level={4}>{INSTRUCTIONS_TITLE}</InstructionsTitle>
            </InstructionsTitleContainer>
            <Timeline>
                {STEPS.map((step, index) => (
                    <Timeline.Item>
                        <StepDescription>
                            <b>Step {index + 1}:</b> {step.description}
                        </StepDescription>
                    </Timeline.Item>
                ))}
            </Timeline>
        </Container>
    );
};
