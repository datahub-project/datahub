import React from 'react';
import { Menu, Dropdown, Button, Checkbox } from 'antd';
import { DownOutlined } from '@ant-design/icons';
import styled from 'styled-components';

interface FilterOption {
    name: string;
    category: string;
    count: number;
    displayName: string;
}

interface FilterGroupOptions {
    [key: string]: FilterOption[];
}

interface AcrylAssertionFiltersProps {
    filterOptions: FilterGroupOptions;
    selectedFilters: FilterOption[];
    onFilterChange: (selectedFilters: FilterOption[]) => void;
}

const StyledSubMenu = styled(Menu.SubMenu)`
    &&& {
        text-transform: capitalize;
    }
    &&& .ant-dropdown-menu-submenu-title {
        color: #374066;
        font-size: 14px;
    }
`;

export const AcrylAssertionFilters: React.FC<AcrylAssertionFiltersProps> = ({
    filterOptions,
    selectedFilters,
    onFilterChange,
}) => {
    const handleFilterChange = (filter: FilterOption, checked: boolean) => {
        const newSelectedFilters = checked
            ? [...selectedFilters, filter]
            : selectedFilters.filter((f) => f.name !== filter.name);

        onFilterChange(newSelectedFilters);
    };

    const checkIsSelected = (filter: FilterOption) => selectedFilters.some((f) => f.name === filter.name);

    const renderSubMenu = (category: string, filters: FilterOption[]) => {
        return filters && filters.length > 0 ? (
            <StyledSubMenu key={category} title={category}>
                {filters.map((filter) => (
                    <Menu.Item key={filter.name}>
                        <Checkbox
                            checked={checkIsSelected(filter)}
                            onChange={(e) => handleFilterChange(filter, e.target.checked)}
                        >
                            {filter.displayName} ({filter.count})
                        </Checkbox>
                    </Menu.Item>
                ))}
            </StyledSubMenu>
        ) : null;
    };

    const menu = (
        <Menu>{Object.entries(filterOptions).map(([category, filters]) => renderSubMenu(category, filters))}</Menu>
    );

    return (
        <Dropdown overlay={menu} trigger={['click']} placement="bottomLeft">
            <Button>
                Filters {selectedFilters.length ? <>&nbsp;({selectedFilters.length})</> : null}
                <DownOutlined />
            </Button>
        </Dropdown>
    );
};
