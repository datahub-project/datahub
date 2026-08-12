import { Folder } from '@phosphor-icons/react/dist/csr/Folder';
import React, { useCallback } from 'react';
import styled, { useTheme } from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import { useOnChangeFilters, useSelectedFilters } from '@app/searchV2/sidebar/SidebarContext';
import {
    type BrowseSearchHit,
    applyBrowseSearchHit,
    browseSearchHitLocation,
} from '@app/searchV2/sidebar/browseSidebarSearch';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import HierarchicalBrowseTreeRow from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseTreeRow';
import SidebarFilteredResults from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarFilteredResults';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataPlatform, EntityType } from '@types';

const PLATFORM_ICON_STYLES = {
    backgroundColor: 'transparent',
    padding: '0px',
    borderRadius: '0px',
};

const HitLocation = styled.span`
    font-size: 12px;
    color: ${(props) => props.theme.colors.textSecondary};
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 120px;
`;

type Props = {
    hits: BrowseSearchHit[];
    loading: boolean;
    isRefreshing: boolean;
    onClear: () => void;
};

function platformIcon(hit: BrowseSearchHit, color: string) {
    if (!hit.platformUrn) {
        return null;
    }
    return (
        <PlatformIcon
            platform={{ urn: hit.platformUrn, type: EntityType.DataPlatform, name: hit.platformName } as DataPlatform}
            size={16}
            color={color}
            styles={PLATFORM_ICON_STYLES}
        />
    );
}

function hitTitle(hit: BrowseSearchHit): string {
    const location = browseSearchHitLocation(hit);
    if (hit.kind === 'platform') {
        return hit.label;
    }
    return [hit.label, hit.platformName, location].filter(Boolean).join(' · ');
}

function BrowseSidebarSearchHitRow({
    hit,
    onSelect,
}: {
    hit: BrowseSearchHit;
    onSelect: (hit: BrowseSearchHit) => void;
}) {
    const theme = useTheme();
    const registry = useEntityRegistry();
    const location = browseSearchHitLocation(hit);
    const logo = platformIcon(hit, theme.colors.icon);

    let icon: React.ReactNode = <Folder size={TREE_ROW_ENTITY_ICON_SIZE} color={theme.colors.icon} />;
    if (hit.kind === 'platform' || hit.kind === 'path') {
        icon = logo ?? icon;
    } else if (hit.kind === 'entity' && hit.entity) {
        icon = registry.getIcon(hit.entity.type, TREE_ROW_ENTITY_ICON_SIZE, IconStyleType.ACCENT, theme.colors.icon);
    }

    return (
        <HierarchicalBrowseTreeRow
            level={0}
            isSelected={false}
            icon={icon}
            label={hit.label}
            labelTitle={hitTitle(hit)}
            trailing={location ? <HitLocation title={location}>{location}</HitLocation> : undefined}
            onSelect={() => onSelect(hit)}
            data-testid={`browse-v2-search-hit-${hit.label}`}
        />
    );
}

const BrowseSidebarSearchResults = ({ hits, loading, isRefreshing, onClear }: Props) => {
    const selectedFilters = useSelectedFilters();
    const onChangeFilters = useOnChangeFilters();

    const handleSelect = useCallback(
        (hit: BrowseSearchHit) => {
            onChangeFilters(applyBrowseSearchHit(hit, selectedFilters));
            onClear();
        },
        [onChangeFilters, onClear, selectedFilters],
    );

    return (
        <SidebarFilteredResults
            count={hits.length}
            loading={loading}
            isRefreshing={isRefreshing}
            onClear={onClear}
            clearTestId="browse-v2-search-clear"
            dataTestId="browse-v2-search-results"
        >
            {hits.map((hit) => (
                <BrowseSidebarSearchHitRow key={hit.key} hit={hit} onSelect={handleSelect} />
            ))}
        </SidebarFilteredResults>
    );
};

export default BrowseSidebarSearchResults;
