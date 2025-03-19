import React from 'react';
import { EditOutlined } from '@ant-design/icons';
import { Button, Tooltip } from '@components';
import styled from 'styled-components';

const StyledButton = styled(Button)`
    height: 28px;
    margin: 0px 4px 0px 4px;
`;

type Props = {
    setShowSelectMode: (showSelectMode: boolean) => any;
    disabled?: boolean;
};

export default function EditButton({ setShowSelectMode, disabled }: Props) {
    return (
        <Tooltip title="Edit..." showArrow={false} placement="top">
            <StyledButton
                onClick={() => setShowSelectMode(true)}
                disabled={disabled}
                data-testid="search-results-edit-button"
            >
                <EditOutlined />
            </StyledButton>
        </Tooltip>
    );
}
