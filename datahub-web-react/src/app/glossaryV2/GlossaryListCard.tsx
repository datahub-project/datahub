import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { GenericEntityProperties } from '@app/entity/shared/types';
import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { Tooltip } from '@src/alchemy-components';

import { EntityType, Maybe } from '@types';

const SmallDescription = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    overflow: hidden;
`;

const StyledIcon = styled(GlossaryColoredIcon)`
    margin-right: 12px;
`;

const EntityDetailsWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 16px 16px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 16px;
    position: relative;
    overflow: hidden;

    &:hover {
        transition: 0.15s;
        background-color: ${(props) => props.theme.colors.bgHover};
    }
`;

const EntityName = styled.div`
    color: ${(props) => props.theme.colors.text};
    font-size: 14px;
    font-weight: bold;
`;

const EntityTitleWrapper = styled.div`
    display: flex;
    align-items: center;
`;

const NameAndDescription = styled.div`
    display: flex;
    flex-direction: column;
    overflow: hidden;
`;

const GlossaryItemCount = styled.span<{ count: number }>`
    display: flex;
    align-items: center;
    gap: 5px;
    border-radius: 20px;
    background: ${(props) => props.theme.colors.bgSurface};
    color: ${(props) => (props.count > 0 ? props.theme.colors.textSecondary : props.theme.colors.textDisabled)};
    padding: 5px 10px;
    width: max-content;
    svg {
        height: 14px;
        width: 14px;
        path {
            fill: ${(props) => (props.count > 0 ? props.theme.colors.icon : props.theme.colors.iconDisabled)};
        }
    }
    border: 1px solid transparent;
    :hover {
        border: 1px solid ${(props) => (props.count > 0 ? props.theme.colors.borderHover : 'transparent')};
    }
`;

const CountText = styled.span`
    font-size: 10px;
    font-weight: 400;
`;

const Icons = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 5px;
`;

const MAX_DESCRIPTION_LENGTH = 100;
const MAX_DEPTH_QUERIED = 4;
const TRUNCATION_SUFFIX = '...';

interface Props {
    name: string;
    description: string | undefined;
    type: EntityType;
    urn: string;
    entityData: GenericEntityProperties | null;
    nodeCount?: Maybe<number>;
    termCount?: Maybe<number>;
    maxDepth?: number;
}

const GlossaryListCard = (props: Props) => {
    const { t } = useTranslation('governance.glossary');
    const { name, description, type, entityData, urn, nodeCount, termCount, maxDepth } = props;
    const isDescriptionTruncated = description && description.length > MAX_DESCRIPTION_LENGTH;
    const truncatedDescription = description?.slice(0, MAX_DESCRIPTION_LENGTH);
    const isExceedingMaxDepth = (maxDepth || 0) > MAX_DEPTH_QUERIED;
    const generateColor = useGenerateGlossaryColorFromPalette();
    const isNode = type === EntityType.GlossaryNode;
    const glossaryColor = generateColor(entityData?.urn || urn);
    const Icon = isNode ? BookmarksSimple : BookmarkSimple;

    return (
        <EntityDetailsWrapper>
            <EntityTitleWrapper>
                <StyledIcon color={glossaryColor} icon={Icon} size={40} iconSize={16} />
                <NameAndDescription>
                    <EntityName>{name}</EntityName>
                    {description && (
                        <SmallDescription>
                            {truncatedDescription}
                            {isDescriptionTruncated ? TRUNCATION_SUFFIX : null}
                        </SmallDescription>
                    )}
                </NameAndDescription>
            </EntityTitleWrapper>
            {isNode ? (
                <Icons>
                    <Tooltip
                        title={t('card.nodeGroupCountTooltip', { count: nodeCount || 0 })}
                        placement="top"
                        showArrow={false}
                    >
                        <GlossaryItemCount count={nodeCount || 0}>
                            <BookmarksSimple />
                            <CountText>
                                {' '}
                                {nodeCount}
                                {isExceedingMaxDepth && `+`}
                            </CountText>
                        </GlossaryItemCount>
                    </Tooltip>
                    <Tooltip
                        title={t('card.termCountTooltip', { count: termCount || 0 })}
                        placement="top"
                        showArrow={false}
                    >
                        <GlossaryItemCount count={termCount || 0}>
                            <BookmarkSimple />
                            <CountText>
                                {' '}
                                {termCount}
                                {isExceedingMaxDepth && `+`}
                            </CountText>
                        </GlossaryItemCount>
                    </Tooltip>
                </Icons>
            ) : null}
        </EntityDetailsWrapper>
    );
};

export default GlossaryListCard;
