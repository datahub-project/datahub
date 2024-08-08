import React from 'react';
import styled from 'styled-components';
import { Popover, Divider } from 'antd';
import {
    getHealthSummaryIcon,
    getHealthSummaryMessage,
    HealthSummaryIconType,
} from '../../../../../shared/health/healthUtils';
import { EntityHealthStatus } from './EntityHealthStatus';
import { Health } from '../../../../../../types.generated';
import { ANTD_GRAY } from '../../../constants';

const Header = styled.span`
    display: flex;
    align-items: center;
    justify-content: left;
`;

const Icon = styled.span`
    margin-right: 8px;
    display: flex;
    align-items: center;
`;

const Title = styled.span`
    font-weight: bold;
    color: ${ANTD_GRAY[1]};
    padding-top: 4px;
    padding-bottom: 4px;
    font-size: 14px;
`;

const StatusContainer = styled.div`
    margin-bottom: 8px;
`;

const StyledDivider = styled(Divider)`
    &&& {
        margin: 0px;
        padding: 0px;
        padding-right: 8px;
        padding-left: 8px;
        margin-top: 8px;
        margin-bottom: 8px;
        border-color: ${ANTD_GRAY[5]};
    }
`;

type Props = {
    health: Health[];
    baseUrl: string;
    children: React.ReactNode;
    fontSize?: number;
    placement?: any;
};

export const EntityHealthPopover = ({ health, baseUrl, children, fontSize, placement = 'right' }: Props) => {
    return (
        <Popover
            content={
                <>
                    <Header>
                        <Icon>{getHealthSummaryIcon(health, HealthSummaryIconType.OUTLINED, fontSize)}</Icon>{' '}
                        <Title>{getHealthSummaryMessage(health)}</Title>
                    </Header>
                    <StyledDivider />
                    {health.map((h) => (
                        <StatusContainer key={h.type}>
                            <EntityHealthStatus type={h.type} message={h.message || undefined} baseUrl={baseUrl} />
                        </StatusContainer>
                    ))}
                </>
            }
            color="#262626"
            placement={placement}
            zIndex={10000000}
        >
            {children}
        </Popover>
    );
};
