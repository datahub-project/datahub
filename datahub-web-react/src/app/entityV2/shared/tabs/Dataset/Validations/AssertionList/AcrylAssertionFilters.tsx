import React from 'react';
import { Menu, Dropdown, Button, Checkbox } from 'antd';
import { CaretDownOutlined } from '@ant-design/icons';

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

    const isSelected = (filter: FilterOption) => selectedFilters.some((f) => f.name === filter.name);

    const renderSubMenu = (category: string, filters: FilterOption[]) => {
        return filters && filters.length > 0 ? (
            <Menu.SubMenu key={category} title={category.toUpperCase()}>
                {filters.map((filter) => (
                    <Menu.Item key={filter.name}>
                        <Checkbox
                            checked={isSelected(filter)}
                            onChange={(e) => handleFilterChange(filter, e.target.checked)}
                        >
                            {filter.displayName} ({filter.count})
                        </Checkbox>
                    </Menu.Item>
                ))}
            </Menu.SubMenu>
        ) : null;
    };

    const menu = (
        <Menu>{Object.entries(filterOptions).map(([category, filters]) => renderSubMenu(category, filters))}</Menu>
    );

    return (
        <Dropdown overlay={menu} trigger={['click']} placement="bottomLeft">
            <Button>
                Filters <CaretDownOutlined />
            </Button>
        </Dropdown>
    );
};
