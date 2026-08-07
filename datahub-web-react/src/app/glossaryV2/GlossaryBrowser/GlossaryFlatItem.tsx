import { Tooltip } from '@components';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { DeprecationIcon } from '@app/entityV2/shared/components/styled/DeprecationIcon';
import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import { resolveGlossaryEntityColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { getGlossaryEntityIcon } from '@app/glossaryV2/utils';
import {
    TREE_ROW_ENTITY_ICON_GLYPH_SIZE,
    TREE_ROW_ENTITY_ICON_SIZE,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, EntityType, GlossaryNode, GlossaryTerm } from '@types';

const RowContainer = styled.div<{ $isSelected: boolean }>`
    position: relative;
    display: flex;
    align-items: center;
    padding: 4px 8px;
    min-height: 38px;
    cursor: pointer;
    border-radius: 6px;
    transition: background-color 0.15s ease;
    margin: 0 2px 2px 2px;

    ${(props) =>
        props.$isSelected &&
        `
        background: ${props.theme.colors.bgSelectedSubtle};
        box-shadow: ${props.theme.colors.shadowFocusBrand};
    `}

    ${(props) =>
        !props.$isSelected &&
        `
        &:hover {
            background: ${props.theme.colors.bgHover};
            box-shadow: ${props.theme.colors.shadowFocus};
        }
    `}
`;

const IconSlot = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 20px;
    margin-right: 8px;
    flex-shrink: 0;
`;

const TextStack = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    min-width: 0;
    overflow: hidden;
`;

const TitleRow = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
    overflow: hidden;
`;

const DeprecationSlot = styled.span`
    display: inline-flex;
    align-items: center;
    flex-shrink: 0;
    line-height: 0;

    & svg {
        width: 12px;
        height: 12px;
    }
`;

const Title = styled.span<{ $isSelected: boolean }>`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 14px;
    line-height: 20px;
    color: ${(props) => props.theme.colors.textSecondary};

    ${(props) =>
        props.$isSelected &&
        `
        background: ${props.theme.colors.brandGradientSelected};
        background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 600;
    `}
`;

const Breadcrumb = styled.span`
    display: flex;
    align-items: center;
    overflow: hidden;
    font-size: 11px;
    line-height: 16px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

const BreadcrumbSegment = styled.span`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    flex: 0 1 auto;
    min-width: 0;
`;

const BreadcrumbSeparator = styled(CaretRight)`
    flex-shrink: 0;
    margin: 0 2px;
`;

interface Props {
    entity: Entity;
}

function getParentNodes(entity: Entity) {
    if (entity.type === EntityType.GlossaryTerm) {
        return (entity as GlossaryTerm).parentNodes?.nodes ?? [];
    }
    if (entity.type === EntityType.GlossaryNode) {
        return (entity as GlossaryNode).parentNodes?.nodes ?? [];
    }
    return [];
}

function getDeprecation(entity: Entity) {
    if (entity.type === EntityType.GlossaryTerm) {
        return (entity as GlossaryTerm).deprecation;
    }
    return undefined;
}

/**
 * Flat-list row when Glossary sidebar filters are active (owners / tags).
 */
export default function GlossaryFlatItem({ entity }: Props) {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { entityData } = useGlossaryEntityData();
    const generateColor = useGenerateGlossaryColorFromPalette();

    const isOnEntityPage = !!entityData && entityData.urn === entity.urn;
    const displayName = entityRegistry.getDisplayName(entity.type, isOnEntityPage ? entityData : entity);
    const deprecation = isOnEntityPage ? entityData?.deprecation : getDeprecation(entity);
    const color = resolveGlossaryEntityColor(entity as GlossaryTerm | GlossaryNode, generateColor);
    const Icon = getGlossaryEntityIcon(entity.type);

    const ancestors = [...getParentNodes(entity)].reverse();
    const ancestorNames = ancestors.map((a) => entityRegistry.getDisplayName(a.type, a)).filter(Boolean);

    const handleClick = () => {
        history.push(entityRegistry.getEntityUrl(entity.type, entity.urn));
    };

    return (
        <RowContainer
            $isSelected={isOnEntityPage}
            onClick={handleClick}
            data-testid={`glossary-flat-item-${entity.urn}`}
        >
            <IconSlot>
                <GlossaryColoredIcon
                    color={color}
                    icon={Icon}
                    size={TREE_ROW_ENTITY_ICON_SIZE}
                    iconSize={TREE_ROW_ENTITY_ICON_GLYPH_SIZE}
                />
            </IconSlot>
            <TextStack>
                <TitleRow>
                    <Tooltip placement="right" title={displayName} mouseEnterDelay={0.1} mouseLeaveDelay={0}>
                        <Title $isSelected={isOnEntityPage}>{displayName}</Title>
                    </Tooltip>
                    {deprecation?.deprecated && (
                        <DeprecationSlot>
                            <DeprecationIcon
                                urn={entity.urn}
                                deprecation={deprecation}
                                showUndeprecate={false}
                                showText={false}
                            />
                        </DeprecationSlot>
                    )}
                </TitleRow>
                {ancestorNames.length > 0 && (
                    <Tooltip
                        placement="bottom"
                        title={ancestorNames.join(' / ')}
                        mouseEnterDelay={0.1}
                        mouseLeaveDelay={0}
                    >
                        <Breadcrumb>
                            {ancestorNames.map((name, idx) => (
                                // eslint-disable-next-line react/no-array-index-key
                                <React.Fragment key={`${entity.urn}-crumb-${idx}`}>
                                    <BreadcrumbSegment>{name}</BreadcrumbSegment>
                                    {idx < ancestorNames.length - 1 && <BreadcrumbSeparator size={10} weight="bold" />}
                                </React.Fragment>
                            ))}
                        </Breadcrumb>
                    </Tooltip>
                )}
            </TextStack>
        </RowContainer>
    );
}
