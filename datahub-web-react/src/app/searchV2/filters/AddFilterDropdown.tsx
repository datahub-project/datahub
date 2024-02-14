/* eslint-disable import/no-cycle */
import React from 'react';
import styled from 'styled-components';
import { Button, Dropdown, Menu, Popover } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { FilterField, FilterPredicate } from './types';
import { DEFAULT_FILTER_FIELDS } from './field/fields';
import { useEntityRegistry } from '../../useEntityRegistry';
import { IconStyleType } from '../../entity/Entity';
import { ANTD_GRAY } from '../../entity/shared/constants';
import ValueMenu from './value/ValueMenu';
import { getDefaultFieldOperatorType } from './value/utils';

const StyledDropdown = styled(Dropdown)``;

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

const ValuePopover = styled(Popover)``;

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
`;

const overlayStyle = { borderRadius: 8, overflow: 'hidden', marginLeft: 12 };

type Props = {
    fields?: FilterField[];
    onAddFilter: (predicate: FilterPredicate) => void;
};

export default function AddFilterDropdown({ fields = DEFAULT_FILTER_FIELDS, onAddFilter }: Props) {
    const entityRegistry = useEntityRegistry();

    const filterFieldOptions = fields.map((field) => {
        const icon =
            field.icon ||
            (field.entityTypes?.length && entityRegistry.getIcon(field.entityTypes[0], 12, IconStyleType.ACCENT));
        return {
            key: field.field,
            label: (
                <ValuePopover
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
                                onChangeValues={(values) =>
                                    onAddFilter({
                                        field,
                                        operator: getDefaultFieldOperatorType(field),
                                        values,
                                        defaultValueOptions: [],
                                    })
                                }
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
                </ValuePopover>
            ),
        };
    });

    return (
        <StyledDropdown trigger={['click']} dropdownRender={(_) => <FieldMenu items={filterFieldOptions} />}>
            <AddFilterButton type="text" icon={<StyledPlusOutlined />}>
                Add filter
            </AddFilterButton>
        </StyledDropdown>
    );
}
