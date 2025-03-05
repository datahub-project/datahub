import React from 'react';

import styled from 'styled-components';
import { Button, Tooltip } from '@components';

const Icon = styled.div`
    margin-right: 8px;
`;

type Props = {
    icon?: React.ReactNode;
    title: string;
    disabled?: boolean;
    tooltip?: React.ReactNode;
    onClick: () => void;
};

export const PrimaryButton = ({ icon, title, tooltip, disabled = false, onClick }: Props) => {
    return (
        <Tooltip title={tooltip} placement="left" showArrow={false}>
            <Button
                disabled={disabled}
                onClick={(e) => {
                    e.stopPropagation();
                    onClick();
                }}
            >
                {(icon && <Icon>{icon}</Icon>) || null}
                {title}
            </Button>
        </Tooltip>
    );
};
