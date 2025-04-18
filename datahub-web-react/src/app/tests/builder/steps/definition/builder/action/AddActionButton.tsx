import React from 'react';
import { Button } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../../../../entity/shared/constants';

const StyledButton = styled(Button)`
    &&& {
        padding: 0px;
        margin: 0px;
        color: ${ANTD_GRAY[8]};
    }
`;

const AddActionText = styled.div`
    :hover {
        text-decoration: underline;
    }
`;

type Props = {
    disabled?: boolean;
    onAddAction: () => void;
};

export const AddActionButton = ({ disabled, onAddAction }: Props) => {
    return (
        <StyledButton type="link" disabled={disabled} onClick={onAddAction}>
            <AddActionText>+ Add action</AddActionText>
        </StyledButton>
    );
};
