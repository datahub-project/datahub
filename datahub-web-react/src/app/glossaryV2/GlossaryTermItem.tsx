import React from 'react';
import { Button, Typography } from 'antd';
import styled from 'styled-components/macro';
import BookmarkIcon from '../../images/collections_bookmark.svg?react';
import GroupBookmarkIconWhite from '../../images/glossary_collections_bookmark_white.svg?react';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import { EntityType, Maybe } from '../../types.generated';
import {
    GlossaryPreviewCardDecoration,
    GLOSSARY_RIBBON_SIZE,
} from '../entityV2/shared/containers/profile/header/GlossaryPreviewCardDecoration';
import { generateColorFromPalette } from './colorUtils';
import { GenericEntityProperties } from '../entity/shared/types';
import { Editor } from '../entityV2/shared/tabs/Documentation/components/editor/Editor';

const { Paragraph } = Typography;

const SmallDescription = styled(Paragraph)`
    color: ${REDESIGN_COLORS.SUB_TEXT};
    font-size: 10px;
    font-weight: 600;
    line-height: 20px;
    margin-top: 10px;
    overflow: auto;
`;

const EntityDetailsLeftColumn = styled.div`
    display: flex;
    gap: 15px;
    align-items: center;
`;

const EntityDetailsRightColumn = styled.div`
    margin-right: 5px;

    svg {
        display: none;
    }
`;

const BookmarkIconWrapper = styled.div<{ urnText: string }>`
    width: 40px;
    height: 40px;
    display: flex;
    justify-content: center;
    align-items: center;
    background-color: ${(props) => generateColorFromPalette(props.urnText)};
    border-radius: 11px;
    margin-right: 10px;
    position: relative;
    overflow: hidden;
`;

const EntityNameWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

const EntityDetails = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-bottom: 1px solid ${REDESIGN_COLORS.LIGHT_GREY};
    padding: 20px 0 20px 0;
    margin: 0 23px 0 19px;
`;

const EntityDetailsWrapper = styled.div`
    width: 100%;
    margin: 0 2px;
    display: flex;
    flex-direction: column;
    padding: ${GLOSSARY_RIBBON_SIZE}px 0 0 ${GLOSSARY_RIBBON_SIZE * 2}px;

    &:hover > ${EntityDetails} > ${EntityDetailsLeftColumn} > ${BookmarkIconWrapper} > svg > g > path {
        transition: 0.15s;
        fill: rgba(216, 160, 75, 1);
    }

    &:hover > ${EntityDetails} > ${EntityDetailsRightColumn} > svg {
        transition: 0.15s;
        display: block;
    }
`;

const EntityName = styled.div`
    color: ${REDESIGN_COLORS.SUBTITLE};
    font-size: 12px;
    font-weight: 400;
`;

const EntityTypeText = styled.div`
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-size: 10px;
    font-weight: 600;
    opacity: 0.5;
`;

const EntityTitleWrapper = styled.div`
    display: flex;
    align-items: center;
`;

const BookmarkRibbon = styled.span<{ urnText: string }>`
    position: absolute;
    left: -11px;
    top: 7px;
    width: 40px;
    transform: rotate(-45deg);
    padding: 4px;
    opacity: 1;
    background-color: rgba(0, 0, 0, 0.2);
`;

const TagsContainer = styled.div`
    display: flex;
    align-items: center;
    margin-top: 15px;
`;

const TagPill = styled.span`
    border-radius: 12px;
    background: ${REDESIGN_COLORS.LIGHT_GREY_PILL};
    padding: 5px 10px;
    display: flex;
    align-items: center;
    font-size: 9px;
    color: ${REDESIGN_COLORS.SUB_TEXT};
`;

const ExpandCollapseButton = styled(Button)`
    font-size: 10px;
    padding: 0;
    height: 10px;
    margin-left: 8px;
`;

const MAX_DESCRIPTION_LENGTH = 150;

interface Props {
    name: string;
    description: string | undefined;
    type: EntityType;
    urn: string;
    entityData: GenericEntityProperties | null;
    count?: Maybe<number>;
}

const GlossaryTermItem = (props: Props) => {
    const { name, description, type, entityData, count } = props;
    const [isExpanded, setIsExpanded] = React.useState(false);
    const isDescriptionTruncated = description && description.length > MAX_DESCRIPTION_LENGTH;
    const truncatedDescription = description?.slice(0, MAX_DESCRIPTION_LENGTH);

    return (
        <EntityDetailsWrapper>
            {type === EntityType.GlossaryNode ? (
                <EntityTitleWrapper>
                    <BookmarkIconWrapper urnText={entityData?.urn || ''}>
                        <BookmarkRibbon urnText={entityData?.urn || ''} />
                        <GroupBookmarkIconWhite />
                    </BookmarkIconWrapper>
                    <EntityNameWrapper>
                        <EntityName>{name}</EntityName>
                        <EntityTypeText>Glossary Term Group</EntityTypeText>
                    </EntityNameWrapper>
                </EntityTitleWrapper>
            ) : (
                <>
                    <GlossaryPreviewCardDecoration urn={entityData?.urn || ''} entityData={entityData} />
                    <EntityName>{name}</EntityName>
                    <EntityTypeText>Glossary Term</EntityTypeText>
                </>
            )}

            {description && (
                <SmallDescription>
                    {isExpanded ? <Editor content={description} readOnly /> : <>{truncatedDescription}...</>}
                    {isDescriptionTruncated && (
                        <ExpandCollapseButton
                            type="link"
                            onClick={(e) => {
                                e.preventDefault();
                                setIsExpanded(!isExpanded);
                            }}
                        >
                            Read {isExpanded ? 'less' : 'more'}
                        </ExpandCollapseButton>
                    )}
                </SmallDescription>
            )}

            {type === EntityType.GlossaryNode && (
                <TagsContainer>
                    <TagPill>
                        <BookmarkIcon width={15} height={15} />
                        {count}
                    </TagPill>
                </TagsContainer>
            )}
        </EntityDetailsWrapper>
    );
};

export default GlossaryTermItem;
