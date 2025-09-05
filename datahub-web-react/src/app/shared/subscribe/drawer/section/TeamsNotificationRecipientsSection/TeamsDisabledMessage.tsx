import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { Text } from '@src/alchemy-components';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';

const LEFT_PADDING = 36;

const DisabledText = styled(Text)`
    font-weight: 500;
    padding-left: ${LEFT_PADDING}px;
    margin-top: 8px;
`;

type TeamsDisabledMessageProps = {
    isAdminAccess: boolean;
};

export default function TeamsDisabledMessage({ isAdminAccess }: TeamsDisabledMessageProps) {
    if (isAdminAccess) {
        return (
            <DisabledText>
                Teams notifications are disabled. In order to enable,{' '}
                <Link to="/settings/integrations/teams" style={{ color: REDESIGN_COLORS.BLUE }}>
                    setup a Teams integration.
                </Link>
            </DisabledText>
        );
    }

    return (
        <DisabledText>
            Teams notifications are disabled. Reach out to your DataHub admins for more information.
        </DisabledText>
    );
}
