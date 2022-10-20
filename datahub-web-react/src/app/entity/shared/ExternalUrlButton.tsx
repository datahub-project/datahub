import { ArrowRightOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { EntityType } from '../../../types.generated';
import analytics, { EventType, EntityActionType } from '../../analytics';

const ExternalUrlWrapper = styled.span`
    font-size: 12px;
`;

const StyledButton = styled(Button)`
    > :hover {
        text-decoration: underline;
    }
    &&& {
        padding-bottom: 0px;
    }
    padding-left: 12px;
    padding-right: 12px;
`;

interface Props {
    externalUrl: string;
    platformName?: string;
    entityUrn: string;
    entityType?: string;
}

export default function ExternalUrlButton({ externalUrl, platformName, entityType, entityUrn }: Props) {
    function sendAnalytics() {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.ClickExternalUrl,
            entityType: entityType as EntityType,
            entityUrn,
        });
    }

    return (
        <ExternalUrlWrapper>
            <StyledButton
                type="link"
                href={externalUrl}
                target="_blank"
                rel="noreferrer noopener"
                onClick={sendAnalytics}
            >
                View in {platformName} <ArrowRightOutlined style={{ fontSize: 12 }} />
            </StyledButton>
        </ExternalUrlWrapper>
    );
}
