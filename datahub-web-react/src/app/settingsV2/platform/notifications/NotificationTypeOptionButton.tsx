import { EditOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

type Props = {
    onClick: () => void;
};

const StyledButton = styled(Button)`
    width: 66px;
    height: 20px;
    padding: 0px;
    margin: 0px;
`;

const EditIcon = styled(EditOutlined)`
    color: #00615f;
    font-size: 16px;
    &:hover {
        color: #30baaa;
    }
`;

export const NotificationTypeOptionsButton = ({ onClick }: Props) => {
    return (
        <StyledButton type="text" onClick={onClick}>
            <EditIcon type="text" />
        </StyledButton>
    );
};
