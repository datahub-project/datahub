import { LoadingOutlined } from '@ant-design/icons';
import { Icon, Pill } from '@components';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

import {
    EXECUTION_REQUEST_STATUS_LOADING,
    EXECUTION_REQUEST_STATUS_RUNNING,
    EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS,
} from '@app/ingestV2/executions/constants';
import {
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusDisplayText,
    getExecutionRequestStatusIcon,
} from '@app/ingestV2/executions/utils';

const STATUS_COLORS = {
    green: '#2F7D32',
    red: '#C62828',
};

const StatusPill = styled(Pill)<{ statusColor: string }>`
    ${({ statusColor }) => {
        const backgroundColor = STATUS_COLORS[statusColor as keyof typeof STATUS_COLORS];
        return (
            backgroundColor &&
            `
            background-color: ${backgroundColor} !important;
            color: white !important;
            border-radius: 4px !important;
            
            > div {
                background-color: ${backgroundColor} !important;
                color: white !important;
                border-radius: 4px !important;
            }
        `
        );
    }}
`;

interface StatusColumnProps {
    status: any;
    onClick?: () => void;
    dataTestId?: string;
}

export function StatusColumn({ status, onClick, dataTestId }: StatusColumnProps) {
    const icon = getExecutionRequestStatusIcon(status);
    const text = getExecutionRequestStatusDisplayText(status) || 'Pending';
    const color = getExecutionRequestStatusDisplayColor(status);

    const iconRenderer = () => {
        if (status === EXECUTION_REQUEST_STATUS_LOADING || status === EXECUTION_REQUEST_STATUS_RUNNING) {
            return <LoadingOutlined />;
        }
        if (status === EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS) {
            return <Icon icon={icon} source="phosphor" size="md" />;
        }
        return <span />;
    };

    return (
        <Button
            type="link"
            onClick={(e) => {
                e.stopPropagation();
                onClick?.();
            }}
            style={{ padding: 0, margin: 0 }}
            data-testid={dataTestId}
        >
            <StatusPill customIconRenderer={iconRenderer} label={text} color={color} size="md" statusColor={color} />
        </Button>
    );
}
