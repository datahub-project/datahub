import React, { useRef } from 'react';
import styled from 'styled-components';
import { Button, Typography } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { ANTD_GRAY, ANTD_GRAY_V2 } from '../../shared/constants';
import { NoMarginButton } from './styledComponents';

const ButtonContainer = styled.div`
    display: flex;
    color: ${ANTD_GRAY[6]};
    flex-direction: column;
`;

const CreateViewButton = styled(NoMarginButton)<{ bordered: boolean }>`
    &&& {
        text-align: left;
        font-weight: normal;
        border-top-left-radius: 0;
        border-top-right-radius: 0;
        margin-left: 8px;
        margin-right: 8px;
        padding-left: 0px;

        ${(props) => props.bordered && `border-top: 1px solid ${ANTD_GRAY_V2[5]};`}
    }
`;

const ManageViewsButton = styled(Button)`
    &&& {
        color: ${(props) => props.theme.styles['primary-color']};
        border-top: 1px solid ${ANTD_GRAY_V2[5]};
        background-color: ${ANTD_GRAY_V2[2]};
        border-top-left-radius: 0;
        border-top-right-radius: 0;
    }
`;

type Props = {
    hasViews: boolean;
    onClickCreateView: () => void;
    onClickManageViews: () => void;
};

export const ViewSelectFooter = ({ hasViews, onClickCreateView, onClickManageViews }: Props) => {
    const manageViewsButtonRef = useRef(null);

    const onHandleClickManageViews = () => {
        (manageViewsButtonRef?.current as any)?.blur();
        onClickManageViews();
    };

    return (
        <ButtonContainer>
            <CreateViewButton
                data-testid="view-select-create"
                type="text"
                bordered={hasViews}
                onClick={onClickCreateView}
            >
                <PlusOutlined />
                <Typography.Text>Create new view</Typography.Text>
            </CreateViewButton>
            <ManageViewsButton type="text" ref={manageViewsButtonRef} onClick={onHandleClickManageViews}>
                Manage Views
            </ManageViewsButton>
        </ButtonContainer>
    );
};
