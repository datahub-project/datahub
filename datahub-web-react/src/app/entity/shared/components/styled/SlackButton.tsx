import React from 'react';
import { SlackOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { Button } from 'antd';

const StyledButton = styled(Button)`
    padding: 8px;
    font-size: 14px;
    margin-left: 18px;
`;

export default function SlackButton() {
    const slackChannelLink = 'http://slack.datahubproject.io';

    return (
        <StyledButton type="primary" href={slackChannelLink} target="_blank" rel="noopener noreferrer">
            <SlackOutlined />
            Live Help
        </StyledButton>
    );
}
