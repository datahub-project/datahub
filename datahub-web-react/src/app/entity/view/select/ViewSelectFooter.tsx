import React, { useRef } from 'react';
import styled from 'styled-components';
import { Button } from 'antd';
import { RightOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../shared/constants';

const ButtonContainer = styled.div`
    display: flex;
    color: ${ANTD_GRAY[6]};
    width: 100%;
    justify-content: left;
`;

const StyledRightOutlined = styled(RightOutlined)`
    && {
        font-size: 8px;
        color: ${ANTD_GRAY[7]};
    }
`;

const ManageViewsButton = styled(Button)`
    font-weight: normal;
    color: ${ANTD_GRAY[7]};
`;

type Props = {
    onClickManageViews: () => void;
};

export const ViewSelectFooter = ({ onClickManageViews }: Props) => {
    const manageViewsButtonRef = useRef(null);

    const onHandleClickManageViews = () => {
        (manageViewsButtonRef?.current as any)?.blur();
        onClickManageViews();
    };

    return (
        <ButtonContainer>
            <ManageViewsButton type="text" ref={manageViewsButtonRef} onClick={onHandleClickManageViews}>
                Manage Views
                <StyledRightOutlined />
            </ManageViewsButton>
        </ButtonContainer>
    );
};
