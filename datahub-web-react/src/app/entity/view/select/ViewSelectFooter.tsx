import { PlusOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React, { useRef } from 'react';
import styled from 'styled-components';

import { NoMarginButton } from '@app/entity/view/select/styledComponents';

const ButtonContainer = styled.div`
    display: flex;
    color: ${(props) => props.theme.colors.textDisabled};
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

        ${(props) => props.bordered && `border-top: 1px solid ${props.theme.colors.border};`}
    }
`;

const ManageViewsButton = styled(Button)`
    &&& {
        color: ${(props) => props.theme.colors.textBrand};
        border-top: 1px solid ${(props) => props.theme.colors.border};
        background-color: ${(props) => props.theme.colors.bgSurface};
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
