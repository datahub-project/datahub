import React from 'react';
import styled from 'styled-components';
import { Dropdown } from 'antd';
import { colors, Text } from '@src/alchemy-components';
import { CaretDown } from '@phosphor-icons/react';
import { LinkItem } from '../../types';
import OptionLabel from './OptionLabel';

const DropdownBase = styled.div`
    text-wrap: nowrap;
    cursor: pointer;
    display: flex;
    gap: 8px;
    align-items: center;
`;

interface Props<T extends LinkItem> {
    linkItems: T[];
}

export default function ViewMoreDropdown<T extends LinkItem>({ linkItems }: Props<T>) {
    const onLinkItemClick = (linkItem: T) => {
        linkItem.attributes.onClick?.();
        window.open(linkItem.url, '_blank', 'noopener,noreferrer');
    };

    return (
        <Dropdown
            trigger={['click']}
            menu={{
                items: linkItems.map((item) => ({
                    label: <OptionLabel text={item.description} />,
                    key: item.key,
                    onClick: () => onLinkItemClick(item),
                })),
            }}
            // FYI: zIndex 1200 to handle the case when dropdown shown in tooltip `HoverEntityTooltip`
            overlayStyle={{ zIndex: 1200 }}
        >
            <DropdownBase>
                <Text size="sm" color="gray" colorLevel={1700}>
                    View more
                </Text>
                <CaretDown size={12} color={colors.gray[1700]} />
            </DropdownBase>
        </Dropdown>
    );
}
