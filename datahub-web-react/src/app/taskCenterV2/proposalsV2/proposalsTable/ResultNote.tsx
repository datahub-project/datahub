import { Avatar, colors, Popover, Text } from '@src/alchemy-components';
import { FileText } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

const PopoverContainer = styled.div`
    display: flex;
    gap: 8px;
    flex-direction: column;
    min-width: 250px;
`;

const IconContainer = styled.div`
    border-radius: 200px;
    border: 1px solid ${colors.gray[100]};
    width: 22px;
    height: 22px;
    display: flex;
    align-items: center;
    justify-content: center;
`;

interface Props {
    resultNote: string;
    authorDisplayName?: string | null;
}

const ResultNote = ({ resultNote, authorDisplayName }: Props) => {
    return (
        <>
            {resultNote && (
                <Popover
                    content={
                        <PopoverContainer>
                            {authorDisplayName && (
                                <div>
                                    <Avatar showInPill name={authorDisplayName} />
                                </div>
                            )}
                            <Text color="gray">{resultNote}</Text>
                        </PopoverContainer>
                    }
                >
                    <IconContainer>
                        <FileText size={12} />
                    </IconContainer>
                </Popover>
            )}
        </>
    );
};

export default ResultNote;
