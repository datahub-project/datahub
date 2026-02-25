import { LoadingOutlined } from '@ant-design/icons';
import { Icon, Pill } from '@components';
import React from 'react';
import styled from 'styled-components';

import {
    EXECUTION_REQUEST_STATUS_LOADING,
    EXECUTION_REQUEST_STATUS_PENDING,
    EXECUTION_REQUEST_STATUS_RUNNING,
} from '@app/ingestV2/executions/constants';
import {
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusDisplayText,
    getExecutionRequestStatusIcon,
} from '@app/ingestV2/executions/utils';

const StatusContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

const AllStatusWrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

const StatusButton = styled.div<{ $clickable?: boolean }>`
    padding: 0px;
    margin: 0px;

    ${(props) =>
        props.$clickable &&
        `
            :hover {
                cursor: pointer;
            }
        `}
`;

interface StatusProps {
    status: any;
    onClick?: () => void;
    dataTestId?: string;
}

export function StatusColumn({ status, onClick, dataTestId }: StatusProps) {
    const icon = getExecutionRequestStatusIcon(status);
    const text = getExecutionRequestStatusDisplayText(status) || 'Pending';
    const color = getExecutionRequestStatusDisplayColor(status);
    return (
        <AllStatusWrapper>
            <StatusContainer>
                <StatusButton
                    data-testid={dataTestId}
                    onClick={(e) => {
                        e.stopPropagation();
                        onClick?.();
                    }}
                    $clickable={status !== EXECUTION_REQUEST_STATUS_PENDING}
                >
                    <Pill
                        customIconRenderer={() =>
                            status === EXECUTION_REQUEST_STATUS_LOADING ||
                            status === EXECUTION_REQUEST_STATUS_RUNNING ? (
                                <LoadingOutlined />
                            ) : (
                                <Icon icon={icon} source="phosphor" size="md" />
                            )
                        }
                        label={text}
                        color={color}
                        size="md"
                    />
                </StatusButton>
            </StatusContainer>
        </AllStatusWrapper>
    );
}
