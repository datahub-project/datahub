import { Button, Icon, Menu, Text, Tooltip } from '@components';
import { ArrowsDownUp } from '@phosphor-icons/react/dist/csr/ArrowsDownUp';
import { Check } from '@phosphor-icons/react/dist/csr/Check';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import type { ItemType } from '@components/components/Menu/types';

export type SidebarSortOption = {
    value: string;
    label: string;
};

type Props = {
    options: SidebarSortOption[];
    value: string;
    onChange: (value: string) => void;
    ariaLabel?: string;
    tooltip?: string;
    dataTestId?: string;
};

const OptionRow = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 8px;
    min-width: 160px;
`;

/**
 * Opt-in sort control for HierarchicalBrowseSidebar — icon + menu.
 * Pages pass their own options; the shell does not hard-code entity fields.
 * Selected check sits on the right (same as SimpleSelect filter options).
 */
export default function SidebarSortSelect({
    options,
    value,
    onChange,
    ariaLabel,
    tooltip,
    dataTestId = 'sidebar-sort-select',
}: Props) {
    const { t } = useTranslation('misc');
    const [open, setOpen] = useState(false);
    const resolvedAriaLabel = ariaLabel ?? t('sidebarSort.ariaLabel');
    const resolvedTooltip = tooltip ?? t('sidebarSort.tooltip');

    const items = useMemo<ItemType[]>(
        () =>
            options.map((option) => {
                const isSelected = option.value === value;
                return {
                    type: 'item' as const,
                    key: option.value,
                    title: option.label,
                    onClick: () => {
                        onChange(option.value);
                        setOpen(false);
                    },
                    dataTestId: `${dataTestId}-option-${option.value}`,
                    render: () => (
                        <OptionRow data-testid={`${dataTestId}-option-${option.value}`}>
                            <Text weight="semiBold" color="gray" colorLevel={600}>
                                {option.label}
                            </Text>
                            {isSelected ? <Icon icon={Check} color="gray" colorLevel={1800} size="lg" /> : null}
                        </OptionRow>
                    ),
                };
            }),
        [options, value, onChange, dataTestId],
    );

    return (
        <Menu open={open} onOpenChange={setOpen} items={items} trigger={['click']} placement="bottomRight">
            <Tooltip title={resolvedTooltip} showArrow={false} placement="bottom">
                <Button
                    variant="text"
                    color="gray"
                    size="lg"
                    isActive={open}
                    icon={{ icon: ArrowsDownUp, color: 'icon' }}
                    aria-label={resolvedAriaLabel}
                    data-testid={dataTestId}
                />
            </Tooltip>
        </Menu>
    );
}
