import React, { useRef, useState } from 'react';
import styled from 'styled-components';
import { RightOutlined } from '@ant-design/icons';
import { Dropdown } from 'antd';
import { MoreFilterOptionLabel } from '../../styledComponents';
import BooleanSearchFilterMenu from './BooleanMoreFilterMenu';
import FilterOption from '../../FilterOption';
import { useElementDimensions } from '../../utils';

const IconNameWrapper = styled.span`
    display: flex;
    align-items: center;
`;

const IconWrapper = styled.span`
    margin-right: 8px;
`;

interface Props {
    icon?: React.ReactNode;
    title: string;
    option: string;
    count: number;
    initialSelected: boolean;
    onUpdate: (newValue: boolean) => void;
}

export default function BooleanMoreFilter({ icon, title, option, count, initialSelected, onUpdate }: Props) {
    const [isMenuOpen, setIsMenuOpen] = useState(false);
    const [isSelected, setIsSelected] = useState<boolean>(initialSelected);
    const labelRef = useRef<HTMLDivElement>(null);
    const { width, height } = useElementDimensions(labelRef);

    function updateSelected() {
        onUpdate(isSelected);
        setIsMenuOpen(false);
    }

    const filterOptions = [
        {
            key: option,
            // Re-use the Normal Filter object
            label: (
                <FilterOption
                    filterOption={{ field: title, value: option, count }}
                    selectedFilterOptions={isSelected ? [{ field: title, value: option }] : []}
                    setSelectedFilterOptions={() => setIsSelected(!isSelected)}
                />
            ),
            style: { padding: 0 },
            displayName: title,
        },
    ];

    return (
        <Dropdown
            trigger={['click']}
            menu={{ items: filterOptions }}
            open={isMenuOpen}
            onOpenChange={(open) => setIsMenuOpen(open)}
            dropdownRender={(menuOption) => (
                <BooleanSearchFilterMenu
                    menuOption={menuOption}
                    onUpdate={updateSelected}
                    style={{ left: width, position: 'absolute', top: -height }}
                />
            )}
        >
            <MoreFilterOptionLabel
                ref={labelRef}
                onClick={() => setIsMenuOpen(!isMenuOpen)}
                isOpen={isMenuOpen}
                isActive={isSelected}
                data-testid={`more-filter-${title}`}
            >
                <IconNameWrapper>
                    {icon && <IconWrapper>{icon}</IconWrapper>}
                    {title} {isSelected ? `(1) ` : ''}
                </IconNameWrapper>
                <RightOutlined style={{ fontSize: '12px', height: '12px' }} />
            </MoreFilterOptionLabel>
        </Dropdown>
    );
}
