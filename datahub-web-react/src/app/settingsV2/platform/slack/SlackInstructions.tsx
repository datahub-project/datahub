import React from 'react';
import styled from 'styled-components';
import { InfoCircleOutlined } from '@ant-design/icons';
import { Card, Typography } from 'antd';
import { REDESIGN_COLORS } from '../../../entity/shared/constants';

const InstructionsCard = styled(Card)`
    padding: 4px;
    margin: 40px;
`;

const InstructionsTitle = styled(Typography.Text)`
    padding: 8px;
    font-size: 12px;
`;

const InstructionsSection = styled.div`
    margin-top: 12px;
    font-size: 12px;
`;

export const SlackInstructions = () => {
    return (
        <InstructionsCard
            title={
                <>
                    <InfoCircleOutlined style={{ color: REDESIGN_COLORS.BLUE }} />
                    <InstructionsTitle strong>Prerequisites</InstructionsTitle>
                </>
            }
        >
            <InstructionsSection>
                Enabling the Slack integration requires that you either generate a Slack <b>App Configuration Token</b>{' '}
                (recommended) or a Slack <b>Bot Token</b>.
            </InstructionsSection>
            <InstructionsSection>
                App Configuration Tokens allow DataHub to automatically connect to Slack upon a Slack admin&apos;s
                approval. Instructions for generating an App Configuration Token can be found
                <a target="_blank" rel="noreferrer" href="https://api.slack.com/authentication/config-tokens#creating">
                    {' '}
                    here
                </a>
                .
            </InstructionsSection>
            <InstructionsSection>
                Bot Tokens require a Slack Admin to first create a Slack app manually. Instructions for generating a
                Slack Bot Token can be found here
                <a
                    target="_blank"
                    rel="noreferrer"
                    href="https://datahubproject.io/docs/managed-datahub/saas-slack-setup"
                >
                    {' '}
                    here
                </a>
                .
            </InstructionsSection>
        </InstructionsCard>
    );
};
