/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Divider, Popover } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { EntityHealthStatus } from '@app/entity/shared/containers/profile/header/EntityHealthStatus';
import { HealthSummaryIconType, getHealthSummaryIcon, getHealthSummaryMessage } from '@app/shared/health/healthUtils';

import { Health } from '@types';

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
