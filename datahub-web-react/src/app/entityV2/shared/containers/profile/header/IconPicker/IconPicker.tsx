import { Input } from '@components';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { FixedSizeGrid as Grid } from 'react-window';
import styled from 'styled-components';

import { PHOSPHOR_ICONS } from '@components/components/Icon/constants';

import {
    PhosphorIconComponent,
    PhosphorIconsModule,
    loadPhosphorIcons,
} from '@app/entityV2/shared/containers/profile/header/IconPicker/loadPhosphorIcons';
import { coloredIconBackground, coloredIconForeground } from '@app/sharedV2/icons/coloredIconMix';

const ICON_SIZE = 24;
const CELL_SIZE = 32;
const CELL_GAP = 8;
const GRID_HEIGHT = 320;

function isRenderableIcon(value: unknown): value is PhosphorIconComponent {
    // Phosphor CSR/root exports are often forwardRef exotic components (typeof === 'object').
    if (typeof value === 'function') {
        return true;
    }
    return typeof value === 'object' && value !== null && '$$typeof' in (value as object);
}

type Props = {
    onIconPick: (icon: string) => void;
    color?: string | null;
    selectedIcon?: string | null;
};

// react-window owns the outer slot (CELL_SIZE + CELL_GAP). Half-gap padding here
// keeps spacing even so the visible hit-target stays CELL_SIZE and is centered.
const CellSlot = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 100%;
    height: 100%;
    box-sizing: border-box;
    padding: ${CELL_GAP / 2}px;
`;

const CellContainer = styled.div<{ $color?: string; selected?: boolean }>`
    display: flex;
    justify-content: center;
    align-items: center;
    width: 100%;
    height: 100%;
    flex-shrink: 0;
    color: ${({ $color, theme }) => ($color ? coloredIconForeground($color, theme.colors.text) : theme.colors.icon)};
    border-radius: 6px;
    /* Inset border (not outline) so the top row isn't clipped by the scroll container. */
    border: 1px solid
        ${({ selected, $color, theme }) => {
            if (!selected) return 'transparent';
            if ($color) return coloredIconForeground($color, theme.colors.text);
            return theme.colors.borderBrand;
        }};
    background: ${({ selected, $color, theme }) => {
        if (selected && $color) return coloredIconBackground($color, theme.colors.bg);
        if (selected) return theme.colors.bgSurface;
        return 'transparent';
    }};
    box-sizing: border-box;
    cursor: pointer;

    &:hover {
        background: ${({ $color, theme }) =>
            $color ? coloredIconBackground($color, theme.colors.bg) : theme.colors.bgSurface};
    }
`;

const StatusState = styled.div`
    height: 100%;
    min-height: ${GRID_HEIGHT - 16}px;
    display: flex;
    align-items: center;
    justify-content: center;
    color: ${({ theme }) => theme.colors.textTertiary};
    font-size: 14px;
`;

const Cell = ({
    columnIndex,
    rowIndex,
    style,
    data,
}: {
    columnIndex: number;
    rowIndex: number;
    style: React.CSSProperties;
    data: {
        iconNames: string[];
        icons: PhosphorIconsModule;
        columnCount: number;
        onIconPick: (icon: string) => void;
        selectedIcon: string | null;
        setSelectedIcon: (icon: string | null) => void;
        color?: string | null;
    };
}) => {
    const { iconNames, icons, columnCount, onIconPick, selectedIcon, setSelectedIcon, color } = data;
    const index = rowIndex * columnCount + columnIndex;
    const iconName = iconNames[index];
    const Icon = iconName ? icons[iconName] : undefined;

    if (!iconName || !isRenderableIcon(Icon)) {
        return <div style={style} />;
    }

    return (
        <div style={style}>
            <CellSlot>
                <CellContainer
                    $color={color || undefined}
                    selected={selectedIcon === iconName}
                    role="button"
                    tabIndex={0}
                    onKeyDown={(e) => {
                        if (e.key === 'Enter') {
                            onIconPick(iconName);
                            setSelectedIcon(iconName);
                        }
                    }}
                    onClick={() => {
                        onIconPick(iconName);
                        setSelectedIcon(iconName);
                    }}
                >
                    <Icon size={ICON_SIZE} color="currentColor" />
                </CellContainer>
            </CellSlot>
        </div>
    );
};

const GridContainer = styled.div`
    height: ${GRID_HEIGHT}px;
    width: 100%;
    margin-top: 12px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 8px;
    overflow: hidden;
    padding: 8px;
    box-sizing: border-box;
`;

export const ChatIconPicker = ({ onIconPick, color, selectedIcon: selectedIconProp }: Props) => {
    const { t } = useTranslation('entity.shared.containers');
    const { t: tf } = useTranslation('common.feedback');
    const containerRef = useRef<HTMLDivElement>(null);
    const [gridWidth, setGridWidth] = useState(0);
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedIcon, setSelectedIcon] = useState<string | null>(selectedIconProp ?? null);
    const [filteredIcons, setFilteredIcons] = useState<string[]>(PHOSPHOR_ICONS);
    const [icons, setIcons] = useState<PhosphorIconsModule | null>(null);
    const [loadError, setLoadError] = useState(false);

    useEffect(() => {
        let cancelled = false;
        loadPhosphorIcons()
            .then((mod) => {
                if (!cancelled) {
                    setIcons(mod);
                    setLoadError(false);
                }
            })
            .catch(() => {
                if (!cancelled) {
                    setIcons(null);
                    setLoadError(true);
                }
            });
        return () => {
            cancelled = true;
        };
    }, []);

    useEffect(() => {
        const node = containerRef.current;
        if (!node) return undefined;

        const updateWidth = () => {
            // Subtract horizontal padding so the virtualized grid fits without a scrollbar.
            setGridWidth(Math.max(0, node.clientWidth - 16));
        };
        updateWidth();
        // Modal layout can settle after the first paint; remeasure once more.
        const raf = window.requestAnimationFrame(updateWidth);

        const observer = new ResizeObserver(updateWidth);
        observer.observe(node);
        return () => {
            window.cancelAnimationFrame(raf);
            observer.disconnect();
        };
    }, [icons]);

    useEffect(() => {
        setSelectedIcon(selectedIconProp ?? null);
    }, [selectedIconProp]);

    useEffect(() => {
        const term = searchTerm.trim().toLowerCase();
        if (!term) {
            setFilteredIcons(PHOSPHOR_ICONS);
            return;
        }
        setFilteredIcons(PHOSPHOR_ICONS.filter((iconName) => iconName.toLowerCase().includes(term)));
    }, [searchTerm]);

    const columnWidth = CELL_SIZE + CELL_GAP;
    const rowHeight = CELL_SIZE + CELL_GAP;
    const columnCount = Math.max(1, Math.floor((gridWidth + CELL_GAP) / columnWidth));

    const cellData = useMemo(
        () => ({
            iconNames: filteredIcons,
            icons: icons ?? {},
            columnCount,
            onIconPick,
            selectedIcon,
            setSelectedIcon,
            color,
        }),
        [filteredIcons, icons, columnCount, onIconPick, selectedIcon, color],
    );

    return (
        <div>
            <Input
                value={searchTerm}
                setValue={setSearchTerm}
                placeholder={t('iconPicker.searchPlaceholder')}
                icon={{ icon: MagnifyingGlass }}
                onClear={() => setSearchTerm('')}
            />
            <GridContainer ref={containerRef}>
                {loadError && (
                    <StatusState>{t('iconPicker.loadFailed', { defaultValue: 'Could not load icons.' })}</StatusState>
                )}
                {!loadError && !icons && <StatusState>{tf('loading')}</StatusState>}
                {!loadError && icons && gridWidth > 0 && (
                    <Grid
                        columnCount={columnCount}
                        columnWidth={columnWidth}
                        height={GRID_HEIGHT - 16}
                        rowCount={Math.ceil(filteredIcons.length / columnCount) || 1}
                        rowHeight={rowHeight}
                        itemData={cellData}
                        width={gridWidth}
                    >
                        {Cell}
                    </Grid>
                )}
            </GridContainer>
        </div>
    );
};
