import { CaretDown } from '@phosphor-icons/react';
import { Dropdown } from 'antd';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import OptionLabel from '@app/entityV2/shared/externalUrl/components/VeiwMoreDropdown/OptionLabel';
import { LinkItem } from '@app/entityV2/shared/externalUrl/types';
import { Text } from '@src/alchemy-components';

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
    const theme = useTheme();
    const onLinkItemClick = (linkItem: T) => {
        linkItem.attributes.onClick?.();
        window.open(linkItem.url, '_blank', 'noopener,noreferrer');
    };

    return (
        <Dropdown
            trigger={['click']}
            menu={{
                items: linkItems.map((item) => ({
                    label: <OptionLabel text={item.description} dataTestId={item.key} />,
                    key: item.key,
                    onClick: () => onLinkItemClick(item),
                })),
            }}
            // FYI: zIndex 1200 to handle the case when dropdown shown in tooltip `HoverEntityTooltip`
            overlayStyle={{ zIndex: 1200 }}
        >
            <DropdownBase data-testid="view-more-dropdown">
                <Text size="sm" color="gray" colorLevel={1700}>
                    View more
                </Text>
                <CaretDown size={12} color={theme.colors.textSecondary} />
            </DropdownBase>
        </Dropdown>
    );
}
