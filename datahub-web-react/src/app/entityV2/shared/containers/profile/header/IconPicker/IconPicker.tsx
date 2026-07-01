import React, { useLayoutEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { FixedSizeGrid as Grid } from 'react-window';
import styled from 'styled-components';

import {
    DOMAIN_ICONS,
    DOMAIN_ICON_LIBRARY,
} from '@app/entityV2/shared/containers/profile/header/IconPicker/domainIconLibrary';
import {
    DEFAULT_MIN_CELL_SIZE,
    buildEffectiveDomainIconList,
    computeIconGridLayout,
    filterDomainIconsBySearch,
} from '@app/entityV2/shared/containers/profile/header/IconPicker/iconPickerUtils';
import { getLazyIcon } from '@app/mfeframework/lazyIconRegistry';
import { SearchBar } from '@src/alchemy-components';

const GRID_HEIGHT = 320;
const ICON_SIZE = 24;

type Props = {
    onIconPick: (icon: string) => void;
    color?: string | null;
    selectedIcon?: string;
    // Optional icons to pin to the top of the grid (before the curated library). Used to
    // preserve visibility of a domain's currently-stored icon when it lives outside our
    // 138-name curated set — e.g. a pre-migration MUI icon that mapped to a rare Phosphor
    // one, or an icon a customer picked before the curated set was introduced. Without
    // this, the picker would show "no selection" for that domain and the user would think
    // their icon disappeared (it hasn't — the domain page still renders it correctly).
    pinnedIcons?: readonly string[];
};

type CellData = {
    iconNames: readonly string[];
    onSelect: (name: string) => void;
    selectedIcon: string;
    color?: string | null;
    columnCount: number;
    cellSize: number;
};

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    width: 100%;
`;

const GridContainer = styled.div`
    border: 1px solid ${({ theme }) => theme.colors.border};
    border-radius: 8px;
    overflow: hidden;
    width: 100%;
`;

const IconCell = styled.div<{ $color?: string | null; $selected: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    cursor: pointer;
    border-radius: 8px;
    color: ${({ $color, theme }) => $color || theme.colors.icon};
    background-color: ${({ $selected, theme }) => ($selected ? theme.colors.bgHover : 'transparent')};
    box-shadow: ${({ $selected, theme }) => ($selected ? `inset 0 0 0 2px ${theme.colors.borderBrand}` : 'none')};

    &:hover {
        background-color: ${({ theme }) => theme.colors.bgHover};
    }
`;

function Cell({
    columnIndex,
    rowIndex,
    style,
    data,
}: {
    columnIndex: number;
    rowIndex: number;
    style: React.CSSProperties;
    data: CellData;
}) {
    const index = rowIndex * data.columnCount + columnIndex;
    const iconName = data.iconNames[index];

    if (!iconName) return <div style={style} />;

    // Curated icons are statically imported — no Suspense, no chunk fetch (see domainIconLibrary.ts).
    // A pinned icon (customer's pre-existing pick outside the curated set) falls back to the
    // one-off lazy loader so we can still show + preserve their selection.
    const CuratedIcon = DOMAIN_ICONS[iconName];

    return (
        <div style={style}>
            <IconCell
                $color={data.color}
                $selected={data.selectedIcon === iconName}
                role="button"
                tabIndex={0}
                aria-label={iconName}
                onClick={() => data.onSelect(iconName)}
                onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        data.onSelect(iconName);
                    }
                }}
                style={{ width: data.cellSize - 8, height: data.cellSize - 8, margin: 4 }}
            >
                {CuratedIcon ? (
                    <CuratedIcon size={ICON_SIZE} weight="regular" />
                ) : (
                    getLazyIcon(iconName, { size: ICON_SIZE, weight: 'regular' })
                )}
            </IconCell>
        </div>
    );
}

export const ChatIconPicker = ({ onIconPick, color, selectedIcon: controlledSelected, pinnedIcons }: Props) => {
    const { t } = useTranslation('entity.shared.containers');
    const [searchTerm, setSearchTerm] = useState('');
    const [uncontrolledSelected, setUncontrolledSelected] = useState<string>('');
    const selectedIcon = controlledSelected ?? uncontrolledSelected;

    const gridWrapperRef = useRef<HTMLDivElement>(null);
    const [containerWidth, setContainerWidth] = useState(0);

    useLayoutEffect(() => {
        const el = gridWrapperRef.current;
        if (!el) return undefined;
        const observer = new ResizeObserver((entries) => {
            const w = entries[0]?.contentRect.width ?? 0;
            setContainerWidth(w);
        });
        observer.observe(el);
        setContainerWidth(el.clientWidth);
        return () => observer.disconnect();
    }, []);

    // Preserve visibility of any pinned icon that (a) is a real Phosphor icon and (b) isn't
    // already in the curated set. Deduped so we never render the same cell twice.
    const effectiveIcons = useMemo(
        () => buildEffectiveDomainIconList(DOMAIN_ICON_LIBRARY, DOMAIN_ICONS, pinnedIcons),
        [pinnedIcons],
    );

    const filteredIconNames = useMemo(
        () => filterDomainIconsBySearch(effectiveIcons, searchTerm),
        [searchTerm, effectiveIcons],
    );

    const handleSelect = (name: string) => {
        onIconPick(name);
        setUncontrolledSelected(name);
    };

    const { columnCount, cellSize } = computeIconGridLayout(containerWidth, DEFAULT_MIN_CELL_SIZE);

    return (
        <Container>
            <SearchBar
                value={searchTerm}
                onChange={setSearchTerm}
                placeholder={t('iconPicker.searchPlaceholder')}
                allowClear
            />
            <GridContainer ref={gridWrapperRef}>
                {containerWidth > 0 && (
                    <Grid
                        columnCount={columnCount}
                        columnWidth={cellSize}
                        height={GRID_HEIGHT}
                        rowCount={Math.ceil(filteredIconNames.length / columnCount)}
                        rowHeight={cellSize}
                        width={containerWidth}
                        itemData={{
                            iconNames: filteredIconNames,
                            onSelect: handleSelect,
                            selectedIcon,
                            color,
                            columnCount,
                            cellSize,
                        }}
                    >
                        {Cell}
                    </Grid>
                )}
            </GridContainer>
        </Container>
    );
};
