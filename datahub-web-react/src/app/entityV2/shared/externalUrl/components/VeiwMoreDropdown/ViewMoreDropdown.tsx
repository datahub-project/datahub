import React from 'react';
import styled from 'styled-components';
import { Dropdown } from 'antd';
import { Text } from '@src/alchemy-components';
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
        >
            <DropdownBase>
                <Text size="sm">View more</Text>
                <CaretDown size={12} />
            </DropdownBase>
        </Dropdown>
    );
}
