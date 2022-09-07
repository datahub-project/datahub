import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

interface Props {
    onClickPrevious?: () => void;
    onClickSkip: () => void;
    onClickNext: () => void;
    isNextDisabled: boolean;
}

export default function StepButtons(props: Props) {
    const { onClickPrevious, onClickSkip, onClickNext, isNextDisabled } = props;

    return (
        <ControlsContainer>
            <Button onClick={onClickPrevious}>Previous</Button>
            <div>
                <Button style={{ marginRight: 8 }} onClick={onClickSkip}>
                    Skip
                </Button>
                <Button disabled={isNextDisabled} onClick={onClickNext}>
                    Next
                </Button>
            </div>
        </ControlsContainer>
    );
}
