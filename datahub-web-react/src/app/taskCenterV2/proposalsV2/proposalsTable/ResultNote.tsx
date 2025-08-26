import { FileText } from 'phosphor-react';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import StopPropagationWrapper from '@app/sharedV2/StopPropagationWrapper';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { Avatar, Popover, Text, colors } from '@src/alchemy-components';

import { CorpUser, EntityType, Maybe } from '@types';

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
    author?: Maybe<CorpUser> | undefined;
}

const ResultNote = ({ resultNote, author }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    const authorDisplayName = author && entityRegistry.getDisplayName(EntityType.CorpUser, author);
    const authorDisplayImage = author && (author.editableInfo?.pictureLink || author.editableProperties?.pictureLink);
    const authorProfileUrl = author && entityRegistry.getEntityUrl(author?.type, author?.urn);
    return (
        <StopPropagationWrapper>
            {resultNote && (
                <Popover
                    content={
                        <PopoverContainer>
                            {authorDisplayName && (
                                <div>
                                    <Link to={authorProfileUrl}>
                                        <Avatar showInPill name={authorDisplayName} imageUrl={authorDisplayImage} />
                                    </Link>
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
        </StopPropagationWrapper>
    );
};

export default ResultNote;
