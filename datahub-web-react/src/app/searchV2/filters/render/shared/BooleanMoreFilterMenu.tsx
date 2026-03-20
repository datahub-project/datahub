import React, { CSSProperties } from 'react';
import styled from 'styled-components/macro';

const DropdownCard = styled.div`
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 12px;
    box-shadow: ${(props) => props.theme.colors.shadowMd};
    overflow: hidden;
    min-width: 200px;
    padding: 4px;
`;

const ScrollableContent = styled.div`
    max-height: 312px;
    overflow: auto;
`;

interface Props {
    filterOption: React.ReactNode;
    onUpdate: () => void;
    style?: CSSProperties;
}

export default function BooleanMoreFilterMenu({ filterOption, onUpdate, style }: Props) {
    return (
        <DropdownCard data-testid="filter-dropdown" style={style} onClick={onUpdate}>
            <ScrollableContent>{filterOption}</ScrollableContent>
        </DropdownCard>
    );
}
