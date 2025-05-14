import * as Icons from '@mui/icons-material';
import { Input } from 'antd';
import React, { useEffect, useState } from 'react';
import { FixedSizeGrid as Grid } from 'react-window';
import styled from 'styled-components';

const columnCount = 5; // Number of columns in the grid

type Props = {
    onIconPick: (icon: string) => void;
    color?: string | null;
};

const CellContainer = styled.div<{ color?: string; selected?: boolean }>`
    display: flex;
    justify-content: center;
    align-items: center;
    color: ${({ color }) => color || 'black'};
    border: ${({ selected }) => (selected ? '2px solid blue' : '1px solid lightgray')};
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
    const Icon = Icons[iconName];

    if (!Icon) return <div style={style} />;

    return (
        <CellContainer
            color={color}
            selected={selectedIcon === iconName}
            style={{
                ...style,
            }}
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
            <Icon />
        </CellContainer>
    );
};

const GridContainer = styled.div`
    height: 400px;
    width: 100%;
    margin-top: 15px;
    border: 1px solid lightgray;
`;

export const ChatIconPicker = ({ onIconPick, color }: Props) => {
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedIcon, setSelectedIcon] = useState<string>('');
    const [filteredIcons, setFilteredIcons] = useState<string[]>([]);

    useEffect(() => {
        const filtered = Object.keys(Icons).filter((iconName) =>
            iconName.toLowerCase().includes(searchTerm.toLowerCase()),
        );
        setFilteredIcons(filtered);
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
            <Input
                type="text"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                placeholder="Search icons..."
            />
            <GridContainer>
                <Grid
                    columnCount={columnCount}
                    columnWidth={91} // Adjust the width as needed
                    height={400}
                    rowCount={Math.ceil(filteredIcons.length / columnCount)}
                    rowHeight={70} // Adjust the height as needed
                    itemData={cellData}
                    width={470}
                >
                    {Cell}
                </Grid>
            </GridContainer>
        </div>
    );
};
