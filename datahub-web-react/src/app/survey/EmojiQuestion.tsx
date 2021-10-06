import React, { ElementType } from 'react';
import styled from 'styled-components';
import { ReactComponent as UpsetEmoji } from '../../images/upset-emoji.svg';
import { ReactComponent as FrowningEmoji } from '../../images/frown-emoji.svg';
import { ReactComponent as OkEmoji } from '../../images/ok-emoji.svg';
import { ReactComponent as SmilingEmoji } from '../../images/smiling-emoji.svg';
import { ReactComponent as GrinningEmoji } from '../../images/grinning-emoji.svg';

const EmojiButton = styled.button`
    width: 45px;
    height: 45px;
    border: none;
    background: none;
`;

const UpsetEmojiStyled = styled(UpsetEmoji)`
    & path {
        fill: white;
    }
    &:hover {
        & path {
            fill: gray;
        }
    }
    width: 45px;
    height: 45px;
`;

const FrowningEmojiStyled = styled(FrowningEmoji)`
    & path {
        fill: white;
    }
    &:hover {
        & path {
            fill: gray;
        }
    }
    width: 45px;
    height: 45px;
`;

const OkEmojiStyled = styled(OkEmoji)`
    & path {
        fill: white;
    }
    &:hover {
        & path {
            fill: gray;
        }
    }
    width: 45px;
    height: 45px;
`;

const SmilingEmojiStyled = styled(SmilingEmoji)`
    & path {
        fill: white;
    }
    &:hover {
        & path {
            fill: gray;
        }
    }
    width: 45px;
    height: 45px;
`;

const GrinningEmojiStyled = styled(GrinningEmoji)`
    & path {
        fill: white;
    }
    stroke: white;
    &:hover {
        & path {
            fill: gray;
        }
        stroke: gray;
    }
    width: 45px;
    height: 45px;
`;

const PaddedRow = styled.div`
    padding-top: 0.5em;
    padding-bottom: 0.5em;
`;

interface Props {
    setResponse1: FunctionStringCallback;
    FlexRow: ElementType;
    QuestionText: ElementType;
}

export const EmojiQuestion = ({ setResponse1, FlexRow, QuestionText }: Props) => {
    return (
        <div>
            <FlexRow>
                <QuestionText>
                    Rate Your Experience
                    <br />
                    <PaddedRow>How&apos;s your experience so far with DataHub?</PaddedRow>
                </QuestionText>
            </FlexRow>
            <FlexRow>
                <EmojiButton type="button" onClick={() => setResponse1('really bad')}>
                    <UpsetEmojiStyled />
                </EmojiButton>
                <EmojiButton type="button" onClick={() => setResponse1('bad')}>
                    <FrowningEmojiStyled />
                </EmojiButton>
                <EmojiButton type="button" onClick={() => setResponse1('ok')}>
                    <OkEmojiStyled />
                </EmojiButton>
                <EmojiButton type="button" onClick={() => setResponse1('good')}>
                    <SmilingEmojiStyled />
                </EmojiButton>
                <EmojiButton type="button" onClick={() => setResponse1('really good')}>
                    <GrinningEmojiStyled />
                </EmojiButton>
            </FlexRow>
        </div>
    );
};
