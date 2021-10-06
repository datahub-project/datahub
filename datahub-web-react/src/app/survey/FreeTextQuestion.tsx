import React, { ElementType } from 'react';
import styled from 'styled-components';

const SubmitButton = styled.input`
    border-style: solid;
    background: #22323d;
    border-color: white;
    border-radius: 8px;
    color: white;
    border-width: 1px;
    float: right;
`;

const SmallTransparentText = styled.p`
    color: lightgray;
    font-size: small;
    margin-bottom: auto;
`;

const FreeTextInput = styled.textarea`
    border-style: solid;
    border-color: white;
    border-width: 1px;
    background: #22323d;
    color: white;
    border-radius: 6px;
    height: 5em;
    margin-top: 1em;
`;

interface Props {
    response3: string;
    setResponse3: FunctionStringCallback;
    FlexRow: ElementType;
    FlexStartRow: ElementType;
    FlexEndRow: ElementType;
    QuestionText: ElementType;
}

export const FreeTextQuestion = ({
    response3,
    setResponse3,
    FlexRow,
    FlexStartRow,
    FlexEndRow,
    QuestionText,
}: Props) => {
    return (
        <div>
            <FlexRow>
                <QuestionText>Leave us your feedback!</QuestionText>
            </FlexRow>
            <FlexRow>
                <FreeTextInput
                    rows={5}
                    cols={60}
                    name="description"
                    maxLength={240}
                    required
                    value={response3}
                    onChange={(e) => setResponse3(e.target.value)}
                />
            </FlexRow>
            <FlexStartRow>
                <SmallTransparentText>{response3.length}/240</SmallTransparentText>
            </FlexStartRow>
            <FlexEndRow>
                <SubmitButton type="submit" value="Submit" />
            </FlexEndRow>
        </div>
    );
};
