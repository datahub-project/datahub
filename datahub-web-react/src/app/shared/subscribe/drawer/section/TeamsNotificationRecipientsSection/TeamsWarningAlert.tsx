import React from 'react';
import styled from 'styled-components/macro';

import { Icon } from '@src/alchemy-components';

const LEFT_PADDING = 36;

const StyledAlert = styled.div`
    margin: 8px 0 0 ${LEFT_PADDING}px;
    padding: 8px 12px;
    background-color: #fffbe6;
    border: 1px solid #ffd666;
    border-radius: 4px;
    color: #d48806;
    display: flex;
    align-items: center;
    gap: 8px;
`;

type TeamsWarningAlertProps = {
    show: boolean;
};

export default function TeamsWarningAlert({ show }: TeamsWarningAlertProps) {
    if (!show) {
        return null;
    }

    return (
        <StyledAlert>
            <Icon icon="WarningCircle" size="sm" />
            Your Teams notifications are currently disabled. Subscribing to this entity will automatically re-enable
            them.
        </StyledAlert>
    );
}
