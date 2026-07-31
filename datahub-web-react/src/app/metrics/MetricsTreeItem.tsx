import type { Icon as PhosphorIcon } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components';

import { DocumentSourceLogo } from '@app/document/DocumentSourceLogo';
import HierarchicalBrowseTreeRow from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseTreeRow';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';

import { DataPlatform } from '@types';

const IconWrapper = styled.div<{ $isSelected: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    flex-shrink: 0;

    && svg {
        color: ${(props) => (props.$isSelected ? props.theme.colors.iconBrand : props.theme.colors.icon)};
    }
`;

interface MetricsTreeItemProps {
    level: number;
    icon: PhosphorIcon;
    platform?: DataPlatform | null;
    title: string;
    isSelected: boolean;
    hasChildren?: boolean;
    childCount?: number;
    isExpanded?: boolean;
    onClick: () => void;
    onToggleExpand?: () => void;
    testId?: string;
}

/** Thin Metrics adapter over HierarchicalBrowseTreeRow. */
export const MetricsTreeItem: React.FC<MetricsTreeItemProps> = ({
    level,
    icon: Icon,
    platform,
    title,
    isSelected,
    hasChildren = false,
    childCount,
    isExpanded = false,
    onClick,
    onToggleExpand,
    testId,
}) => {
    const glyph = (
        <IconWrapper $isSelected={isSelected}>
            {platform ? (
                <DocumentSourceLogo
                    platform={platform}
                    size={16}
                    fallback={<Icon size={TREE_ROW_ENTITY_ICON_SIZE} weight={isSelected ? 'fill' : 'regular'} />}
                />
            ) : (
                <Icon size={TREE_ROW_ENTITY_ICON_SIZE} weight={isSelected ? 'fill' : 'regular'} />
            )}
        </IconWrapper>
    );

    return (
        <HierarchicalBrowseTreeRow
            level={level}
            isSelected={isSelected}
            hasChildren={hasChildren}
            isExpanded={isExpanded}
            count={childCount}
            icon={glyph}
            label={title}
            labelTitle={title}
            onSelect={onClick}
            onToggleExpand={onToggleExpand}
            data-testid={testId}
        />
    );
};
