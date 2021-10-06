import React, { ElementType } from 'react';
import styled from 'styled-components';

const PrimaryButton = styled.button`
    border-style: solid;
    background: #22323d;
    border-color: white;
    border-radius: 8px;
    color: white;
    border-width: 1px;
    padding: 5px 20px;
`;

const PaddingTop = styled.div`
    padding-top: 1em;
`;

interface Props {
    setResponse2: FunctionStringCallback;
    FlexRow: ElementType;
    QuestionText: ElementType;
    Text: ElementType;
}

export const YesNoQuestion = ({ setResponse2, FlexRow, QuestionText, Text }: Props) => {
    return (
        <div>
            <FlexRow>
                <QuestionText>Were you able to find what you were looking for?</QuestionText>
            </FlexRow>
            <PaddingTop>
                <FlexRow>
                    <PrimaryButton type="button" onClick={() => setResponse2('no')}>
                        <Text>No</Text>
                    </PrimaryButton>
                    <PrimaryButton type="button" onClick={() => setResponse2('yes')}>
                        <Text>Yes</Text>
                    </PrimaryButton>
                </FlexRow>
            </PaddingTop>
        </div>
    );
};
