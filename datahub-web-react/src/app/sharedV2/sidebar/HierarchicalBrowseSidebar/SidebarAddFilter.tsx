import { Button, Menu } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import type { ItemType } from '@components/components/Menu/types';

export type SidebarAddFilterOption = {
    value: string;
    label: string;
};

type Props = {
    options: SidebarAddFilterOption[];
    onAdd: (value: string) => void;
    label?: string;
    dataTestId?: string;
};

const AddFilterButton = styled(Button)`
    padding: 4px 8px;
    gap: 4px;
    font-weight: 500;
`;

/** Wait for the menu overlay to unmount before mounting/opening the new filter. */
const AFTER_MENU_CLOSE_MS = 50;

/**
 * Notion-style "+ Filter" — menu of secondary filter types to promote into the
 * filter row. Hide when `options` is empty (everything already promoted).
 *
 * Defers `onAdd` until after the menu closes so the new select's defaultOpen
 * isn't killed by the menu's outside-click teardown.
 */
export default function SidebarAddFilter({ options, onAdd, label, dataTestId = 'sidebar-add-filter' }: Props) {
    const { t } = useTranslation('misc');
    const [open, setOpen] = useState(false);
    const resolvedLabel = label ?? t('sidebarAddFilter.label');

    const items = useMemo<ItemType[]>(
        () =>
            options.map((option) => ({
                type: 'item' as const,
                key: option.value,
                title: option.label,
                onClick: () => {
                    setOpen(false);
                    window.setTimeout(() => onAdd(option.value), AFTER_MENU_CLOSE_MS);
                },
                dataTestId: `${dataTestId}-option-${option.value}`,
            })),
        [options, onAdd, dataTestId],
    );

    if (options.length === 0) return null;

    return (
        <Menu open={open} onOpenChange={setOpen} items={items} trigger={['click']} placement="bottomLeft">
            <AddFilterButton
                variant="text"
                color="gray"
                size="sm"
                isActive={open}
                icon={{ icon: Plus, color: 'icon' }}
                data-testid={dataTestId}
            >
                {resolvedLabel}
            </AddFilterButton>
        </Menu>
    );
}
