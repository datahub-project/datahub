import { Icon, Text } from '@components';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';
import styled from 'styled-components';

import { MenuItemType } from '@components/components/Menu/types';
import { spacing } from '@components/theme';

const Wrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: ${spacing.xsm};
    padding: ${spacing.xsm};
`;

const Caret = styled.div`
    display: flex;
    color: ${(props) => props.theme.colors.icon};
`;

/**
 * Menu item row for the filter menus (More, Add filter). Matches the option rows in the filter
 * dropdowns rather than the Menu default's semibold title.
 */
export function FilterMenuItem({ item }: { item: MenuItemType }) {
    return (
        <Wrapper data-testid={item.dataTestId || `menu-item-${item.key}`}>
            <Text color="text" weight="medium">
                {item.title}
            </Text>
            {item.children && (
                <Caret>
                    <Icon icon={CaretRight} size="lg" />
                </Caret>
            )}
        </Wrapper>
    );
}

export const renderFilterMenuItem = (item: MenuItemType) => <FilterMenuItem item={item} />;
