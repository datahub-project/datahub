import { Input } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { FixedSizeGrid as Grid } from 'react-window';
import styled from 'styled-components';

import { PHOSPHOR_ICONS } from '@components/components/Icon/constants';

import { getLazyIcon } from '@app/mfeframework/lazyIconRegistry';

const columnCount = 5; // Number of columns in the grid

type Props = {
    onIconPick: (icon: string) => void;
    color?: string | null;
    selectedIcon?: string | null;
};

const CellContainer = styled.div<{ color?: string; selected?: boolean }>`
    display: flex;
    justify-content: center;
    align-items: center;
    color: ${({ color, theme }) => color || theme.colors.icon};
    border: ${({ selected, theme }) =>
        selected ? `2px solid ${theme.colors.borderBrand}` : `1px solid ${theme.colors.border}`};
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
        onIconPick: (icon: string) => void;
        selectedIcon: string;
        setSelectedIcon: (icon: string) => void;
        color?: string | null;
    };
}) => {
    const { iconNames, onIconPick, selectedIcon, setSelectedIcon, color } = data;
    const index = rowIndex * columnCount + columnIndex;
    const iconName = iconNames[index];

    if (!iconName) return <div style={style} />;

    return (
        <CellContainer
            color={color || undefined}
            selected={selectedIcon === iconName}
            style={style}
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
            {getLazyIcon(iconName, { size: 28 })}
        </CellContainer>
    );
};

const GridContainer = styled.div`
    height: 400px;
    width: 100%;
    margin-top: 15px;
    border: 1px solid lightgray;
`;

export const ChatIconPicker = ({ onIconPick, color, selectedIcon: selectedIconProp }: Props) => {
    const { t } = useTranslation('entity.shared.containers');
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedIcon, setSelectedIcon] = useState<string>(selectedIconProp || '');
    const [filteredIcons, setFilteredIcons] = useState<string[]>(PHOSPHOR_ICONS);

    useEffect(() => {
        if (selectedIconProp) {
            setSelectedIcon(selectedIconProp);
        }
    }, [selectedIconProp]);

    useEffect(() => {
        const term = searchTerm.trim().toLowerCase();
        if (!term) {
            setFilteredIcons(PHOSPHOR_ICONS);
            return;
        }
        setFilteredIcons(PHOSPHOR_ICONS.filter((iconName) => iconName.toLowerCase().includes(term)));
    }, [searchTerm]);

    const cellData = useMemo(
        () => ({
            iconNames: filteredIcons,
            onIconPick,
            selectedIcon,
            setSelectedIcon,
            color,
        }),
        [filteredIcons, onIconPick, selectedIcon, color],
    );

    return (
        <div>
            <Input
                type="text"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                placeholder={t('iconPicker.searchPlaceholder')}
            />
            <GridContainer>
                <Grid
                    columnCount={columnCount}
                    columnWidth={91}
                    height={400}
                    rowCount={Math.ceil(filteredIcons.length / columnCount)}
                    rowHeight={70}
                    itemData={cellData}
                    width={470}
                >
                    {Cell}
                </Grid>
            </GridContainer>
        </div>
    );
};
