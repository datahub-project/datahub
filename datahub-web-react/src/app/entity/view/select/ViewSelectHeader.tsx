import React, { useRef } from 'react';
import styled from 'styled-components';
import { NoMarginButton } from './styledComponents';
import { ANTD_GRAY_V2 } from '../../shared/constants';

const ButtonContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

const AllEntitiesButton = styled(NoMarginButton)`
    &&& {
        font-weight: normal;
        border-bottom: 1px solid ${ANTD_GRAY_V2[5]};
        width: 100%;
        text-align: left;
        border-bottom-left-radius: 0;
        border-bottom-right-radius: 0;
        margin-left: 8px;
        margin-right: 8px;
        padding-left: 0px;
    }
`;

type Props = {
    onClickClear: () => void;
};

export const ViewSelectHeader = ({ onClickClear }: Props) => {
    const clearButtonRef = useRef(null);

    const onHandleClickClear = () => {
        (clearButtonRef?.current as any)?.blur();
        onClickClear();
    };

    return (
        <ButtonContainer>
            <AllEntitiesButton
                data-testid="view-select-clear"
                type="text"
                ref={clearButtonRef}
                onClick={onHandleClickClear}
            >
                View all
            </AllEntitiesButton>
        </ButtonContainer>
    );
};
