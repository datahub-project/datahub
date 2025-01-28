/* eslint-disable import/no-cycle */
import { PlusOutlined } from '@ant-design/icons';
import { Button, Dropdown, Menu } from 'antd';
import { Popover } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';
import { IconStyleType } from '../../entity/Entity';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import { DEFAULT_FILTER_FIELDS } from './field/fields';
import { FieldType, FilterField, FilterPredicate } from './types';
import { getDefaultFieldOperatorType } from './value/utils';
import ValueMenu from './value/ValueMenu';

const StyledPlusOutlined = styled(PlusOutlined)`
    && {
        font-size: 12px;
    }
`;

const FieldMenu = styled(Menu)`
    max-height: 400px;
    overflow: auto;
    border-radius: 8px;

    &&& {
        .ant-dropdown-menu-item {
            border-radius: 8px;
            margin: 4px 8px;
        }

        .ant-dropdown-menu-submenu-expand-icon {
            display: none;
        }
    }
`;

const ValueMenuWrapper = styled.div``;

const Icon = styled.div`
    margin-right: 8px;

    && {
        color: ${ANTD_GRAY[7]};
    }
`;

const Text = styled.div`
    font-size: 14px;
`;

const Option = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    padding: 2px 0px;
`;

const AddFilterButton = styled(Button)`
    margin: 0px;
    padding: 4px;
    width: fit-content;
`;

const overlayStyle = { borderRadius: 8, overflow: 'hidden', marginLeft: 12 };

interface Props {
    fields?: FilterField[];
    onAddFilter: (predicate: FilterPredicate) => void;
    includeCount?: boolean;
}

export default function AddFilterDropdown({ fields = DEFAULT_FILTER_FIELDS, onAddFilter, includeCount }: Props) {
    const [dropdownOpen, setDropdownOpen] = useState(false);

    const items = fields.map((field) => {
        return {
            key: field.field,
            label: (
                <FilterPopover
                    field={field}
                    onAddFilter={onAddFilter}
                    setDropdownOpen={setDropdownOpen}
                    includeCount={includeCount}
                />
            ),
        };
    });

    return (
        <Dropdown
            open={dropdownOpen}
            onOpenChange={setDropdownOpen}
            trigger={['click']}
            menu={{ items }}
            dropdownRender={(menu) => <FieldMenu>{menu}</FieldMenu>}
        >
            <AddFilterButton type="text" icon={<StyledPlusOutlined />}>
                Add filter
            </AddFilterButton>
        </Dropdown>
    );
}

interface PopoverProps {
    field: FilterField;
    onAddFilter: (predicate: FilterPredicate) => void;
    setDropdownOpen: (open: boolean) => void;
    includeCount?: boolean;
}

function FilterPopover({ field, onAddFilter, setDropdownOpen, includeCount }: PopoverProps) {
    const [popoverOpen, setPopoverOpen] = useState(false);
    const entityRegistry = useEntityRegistry();

    const icon =
        field.icon ||
        (field.type === FieldType.ENTITY &&
            field.entityTypes?.length &&
            entityRegistry.getIcon(field.entityTypes[0], 12, IconStyleType.ACCENT, ANTD_GRAY[7]));

    return (
        <Popover
            open={popoverOpen}
            onOpenChange={setPopoverOpen}
            showArrow={false}
            placement="rightTop"
            trigger="click"
            overlayClassName="search-filter-popover"
            overlayInnerStyle={overlayStyle}
            content={
                <ValueMenuWrapper onClick={(e) => e?.stopPropagation()}>
                    <ValueMenu
                        field={field}
                        values={[]}
                        defaultOptions={[]}
                        includeCount={includeCount}
                        onChangeValues={(values) => {
                            onAddFilter({
                                field,
                                operator: getDefaultFieldOperatorType(field),
                                values,
                                defaultValueOptions: [],
                            });
                            setDropdownOpen(false);
                            setPopoverOpen(false);
                        }}
                        type="default"
                        visible
                    />
                </ValueMenuWrapper>
            }
        >
            <Option onClick={(e) => e?.stopPropagation()}>
                {icon && <Icon>{icon}</Icon>}
                <Text>{field.displayName}</Text>
            </Option>
        </Popover>
    );
}
