import { FormOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../entity/shared/constants';

type Props = {
    onClick: () => void;
};

const StyledButton = styled(Button)`
    width: 66px;
    height: 20px;
    padding: 0px;
    margin: 0px;
`;

export const NotificationTypeOptionsButton = ({ onClick }: Props) => {
    return (
        <StyledButton type="text" onClick={onClick}>
            <FormOutlined style={{ color: ANTD_GRAY[8] }} />
        </StyledButton>
    );
};
