import React, { useRef } from 'react';
import styled from 'styled-components';
import { Button, Typography } from 'antd';
import { PlusOutlined } from '@ant-design/icons';

const ButtonContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

const NoMarginButton = styled(Button)`
    && {
        margin: 0px;
    }
`;

type Props = {
    onClickCreateView: () => void;
    onClickClear: () => void;
};

export const ViewSelectHeader = ({ onClickCreateView, onClickClear }: Props) => {
    const clearButtonRef = useRef(null);

    const onHandleClickClear = () => {
        (clearButtonRef?.current as any)?.blur();
        onClickClear();
    };

    return (
        <ButtonContainer>
            <NoMarginButton data-testid="view-select-create" type="text" onClick={onClickCreateView}>
                <PlusOutlined />
                <Typography.Text strong> Create View</Typography.Text>
            </NoMarginButton>
            <NoMarginButton
                data-testid="view-select-clear"
                type="link"
                ref={clearButtonRef}
                onClick={onHandleClickClear}
            >
                Clear
            </NoMarginButton>
        </ButtonContainer>
    );
};
