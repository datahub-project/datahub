import { Icon, Pill } from '@components';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

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

const StatusButton = styled(Button)`
    padding: 0px;
    margin: 0px;
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
                    type="link"
                    onClick={(e) => {
                        e.stopPropagation();
                        onClick?.();
                    }}
                >
                    <Pill
                        customIconRenderer={() => <Icon icon={icon} source="phosphor" size="md" />}
                        label={text}
                        color={color}
                        size="md"
                    />
                </StatusButton>
            </StatusContainer>
        </AllStatusWrapper>
    );
}
