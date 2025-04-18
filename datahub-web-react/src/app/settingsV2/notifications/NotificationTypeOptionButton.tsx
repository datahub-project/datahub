import React from 'react';
import { EditOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { Button } from '@src/alchemy-components';

type Props = {
    onClick: () => void;
};

const StyledButton = styled(Button)`
    height: 20px;
    padding: 0px;
    margin: 0px;
`;

const EditIcon = styled(EditOutlined)`
    font-size: 16px;
`;

export const NotificationTypeOptionsButton = ({ onClick }: Props) => {
    return (
        <StyledButton data-testid="edit-notification-setting-button" variant="text" onClick={onClick}>
            <EditIcon />
        </StyledButton>
    );
};
