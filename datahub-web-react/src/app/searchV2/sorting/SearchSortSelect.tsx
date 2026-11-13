import { Button, Menu, Tooltip } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ItemType } from '@components/components/Menu/types';

import { DEFAULT_SORT_OPTION } from '@app/searchV2/context/constants';
import useGetSortOptions from '@app/searchV2/sorting/useGetSortOptions';

const SortButton = styled(Button)`
    font-weight: 700;
    color: ${(props) => props.theme.colors.textTertiary};

    &:hover {
        color: ${(props) => props.theme.colors.text};
    }
`;

type Props = {
    selectedSortOption: string | undefined;
    setSelectedSortOption: (option: string) => void;
};

export default function SearchSortSelect({ selectedSortOption, setSelectedSortOption }: Props) {
    const { t } = useTranslation('search');
    const sortOptions = useGetSortOptions();

    const items: ItemType[] = useMemo(
        () =>
            Object.entries(sortOptions).map(([value, option]) => ({
                type: 'item',
                key: value,
                title: option.label,
                onClick: () => setSelectedSortOption(value),
            })),
        [sortOptions, setSelectedSortOption],
    );

    // The default sort is implicit, so the trigger falls back to the placeholder rather than
    // naming it — only an explicit, non-default choice is surfaced on the button.
    const triggerLabel =
        selectedSortOption && selectedSortOption !== DEFAULT_SORT_OPTION
            ? sortOptions[selectedSortOption]?.label
            : undefined;

    return (
        <Tooltip title={t('sort.tooltipTitle')} showArrow={false} placement="left">
            <Menu items={items} trigger={['click']} placement="bottomRight">
                <SortButton
                    variant="text"
                    color="gray"
                    icon={{ icon: CaretDown, size: 'lg' }}
                    iconPosition="right"
                    data-testid="search-sort-select"
                >
                    {triggerLabel ?? t('sort.placeholder')}
                </SortButton>
            </Menu>
        </Tooltip>
    );
}
