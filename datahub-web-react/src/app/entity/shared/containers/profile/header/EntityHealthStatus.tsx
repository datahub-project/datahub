/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { ANTD_GRAY, REDESIGN_COLORS } from '@app/entity/shared/constants';
import { getHealthRedirectPath, getHealthTypeName } from '@app/shared/health/healthUtils';

import { HealthStatusType } from '@types';

const StatusContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    color: ${ANTD_GRAY[1]};
    font-size: 14px;
`;

const Title = styled.span`
    display: flex;
    align-items: center;
    font-weight: bold;
    margin-right: 8px;
    width: 72px;
`;

const RedirectLink = styled(Link)`
    margin-left: 4px;
    color: ${REDESIGN_COLORS.BLUE};
`;

type Props = {
    type: HealthStatusType;
    message?: string | undefined;
    baseUrl: string;
};

export const EntityHealthStatus = ({ type, message, baseUrl }: Props) => {
    const title = getHealthTypeName(type);
    const redirectPath = getHealthRedirectPath(type);
    const fullPath = `${baseUrl}/${redirectPath}`;
    return (
        <StatusContainer>
            <Title>{title}</Title> {message}
            {redirectPath && (
                <RedirectLink to={fullPath} data-testid={`${title.toLowerCase()}-details`}>
                    details
                </RedirectLink>
            )}
        </StatusContainer>
    );
};
