import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import StripMarkdownText, { removeMarkdown } from '../../../components/styled/StripMarkdownText';

const Notes = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
`;

const ReadLessText = styled(Typography.Link)`
    margin-right: 2px;
`;

const StyledViewer = styled(Typography.Paragraph)`
    padding-right: 5px;
    padding-top: 6px !important;
`;

type NotesProps = {
    onExpanded: (expanded: boolean) => void;
    expanded: boolean;
    notes: string;
};

const ABBREVIATED_LIMIT = 100;

export default function MetricNotesField({
    expanded,
    onExpanded: handleExpanded,
    notes,
}: NotesProps) {
    const overLimit = removeMarkdown(notes).length > ABBREVIATED_LIMIT;

    return (
        <Notes>
            {expanded || !overLimit ? (
                <>
                    {!!notes && <StyledViewer>{notes}</StyledViewer>}
                    {!!notes && (
                        <>
                            {overLimit && (
                                <ReadLessText
                                    onClick={() => {
                                        handleExpanded(false);
                                    }}
                                >
                                    Read Less
                                </ReadLessText>
                            )}
                        </>
                    )}
                </>
            ) : (
                <>
                    <StripMarkdownText
                        limit={ABBREVIATED_LIMIT}
                        readMore={
                            <>
                                <Typography.Link
                                    onClick={() => {
                                        handleExpanded(true);
                                    }}
                                >
                                    Read More
                                </Typography.Link>
                            </>
                        }
                        shouldWrap
                    >
                        {notes}
                    </StripMarkdownText>
                </>
            )}
        </Notes>
    );
}
