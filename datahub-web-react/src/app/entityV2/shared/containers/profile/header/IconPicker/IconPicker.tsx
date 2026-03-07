import * as PhosphorIcons from '@phosphor-icons/react';
import React, { useMemo, useState } from 'react';
import { FixedSizeGrid as Grid } from 'react-window';
import styled from 'styled-components';

import { DOMAIN_ICON_NAMES } from '@app/entityV2/shared/containers/profile/header/IconPicker/domainIconList';

const columnCount = 5;

type Props = {
    onIconPick: (icon: string) => void;
    color?: string | null;
};

const StyledInput = styled.input`
    width: 100%;
    padding: 8px 12px;
    font-size: 14px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 6px;
    outline: none;
    transition: border-color 0.2s;

    &:focus {
        border-color: ${(props) => props.theme.colors.textInformation};
        box-shadow: ${(props) => props.theme.colors.shadowFocusBrand};
    }
`;

const CellContainer = styled.div<{ color?: string; selected?: boolean }>`
    display: flex;
    justify-content: center;
    align-items: center;
    color: ${({ color, theme }) => color || theme.colors.text};
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
    style: any;
    data: any;
}) => {
    const { icons, onIconPick, selectedIcon, setSelectedIcon, color } = data;
    const index = rowIndex * columnCount + columnIndex;
    const iconName = icons[index];
    if (!iconName) return <div style={style} />;

    const Icon = PhosphorIcons[iconName] as React.ElementType | undefined;
    if (!Icon) return <div style={style} />;

    return (
        <CellContainer
            color={color}
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
            <Icon size={32} />
        </CellContainer>
    );
};

const GridContainer = styled.div`
    height: 400px;
    width: 100%;
    margin-top: 15px;
    border: 1px solid ${(props) => props.theme.colors.border};
`;

export const ChatIconPicker = ({ onIconPick, color }: Props) => {
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedIcon, setSelectedIcon] = useState<string>('');

    const filteredIcons = useMemo(() => {
        if (!searchTerm) return DOMAIN_ICON_NAMES;
        const term = searchTerm.toLowerCase();
        return DOMAIN_ICON_NAMES.filter((name) => name.toLowerCase().includes(term));
    }, [searchTerm]);

    const cellData = {
        icons: filteredIcons,
        onIconPick,
        selectedIcon,
        setSelectedIcon,
        color,
    };

    return (
        <div>
            <StyledInput
                type="text"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                placeholder="Search icons..."
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
