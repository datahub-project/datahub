import { RightOutlined } from '@ant-design/icons';
import { Dropdown } from 'antd';
import React, { useRef, useState } from 'react';
import styled from 'styled-components';

import FilterOption from '@app/searchV2/filters/FilterOption';
import BooleanSearchFilterMenu from '@app/searchV2/filters/render/shared/BooleanMoreFilterMenu';
import { MoreFilterOptionLabel } from '@app/searchV2/filters/styledComponents';
import { useElementDimensions } from '@app/searchV2/filters/utils';

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
    const { width, height, isElementOutsideWindow } = useElementDimensions(labelRef);

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
                    style={{
                        position: 'absolute',
                        top: -height,
                        [isElementOutsideWindow ? 'right' : 'left']: width,
                    }}
                />
            )}
        >
            <MoreFilterOptionLabel
                onClick={() => setIsMenuOpen(!isMenuOpen)}
                isOpen={isMenuOpen}
                $isActive={isSelected}
                data-testid={`more-filter-${title.replace(/\s/g, '-')}`}
                ref={labelRef}
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
