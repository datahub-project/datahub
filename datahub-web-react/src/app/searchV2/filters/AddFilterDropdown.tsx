/* eslint-disable import/no-cycle */
import { Button, Icon, Menu } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useCallback, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ItemType } from '@components/components/Menu/types';

import { renderFilterMenuItem } from '@app/searchV2/filters/FilterMenuItem';
import { DEFAULT_FILTER_FIELDS } from '@app/searchV2/filters/field/fields';
import { FilterField, FilterPredicate } from '@app/searchV2/filters/types';
import { useFilterMenuOverlay } from '@app/searchV2/filters/useFilterMenuOverlay';
import { FilterMenuProvider, useFilterMenuCommits } from '@app/searchV2/filters/value/FilterMenuContext';
import ValueMenu from '@app/searchV2/filters/value/ValueMenu';
import { getDefaultFieldOperatorType } from '@app/searchV2/filters/value/utils';
import { PARENT_DOCUMENT_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useIsContextDocumentsEnabled } from '@app/useAppConfig';

const AddFilterButton = styled(Button)`
    margin: 0px;
    padding: 4px;
    width: fit-content;
`;

// Alchemy Button does not forward refs, so the menu is measured against this wrapper.
const TriggerAnchor = styled.span`
    display: inline-flex;
`;

interface Props {
    fields?: FilterField[];
    onAddFilters: (predicates: FilterPredicate[]) => void;
    includeCount?: boolean;
}

export default function AddFilterDropdown({ fields = DEFAULT_FILTER_FIELDS, onAddFilters, includeCount }: Props) {
    const { t } = useTranslation('search');
    const isContextDocumentsEnabled = useIsContextDocumentsEnabled();
    const [isOpen, setIsOpen] = useState(false);
    const { triggerRef, overlayClassName, overlayStyle } = useFilterMenuOverlay<HTMLSpanElement>(isOpen);

    const { registerCommit, commitAll } = useFilterMenuCommits();
    // Submenus commit only while the menu closes; their new filters are collected and added at once.
    const pendingPredicatesRef = useRef<FilterPredicate[]>([]);

    const handleAddFilter = useCallback((field: FilterField, values: any[]) => {
        pendingPredicatesRef.current.push({
            field,
            operator: getDefaultFieldOperatorType(field),
            values,
            defaultValueOptions: [],
        });
    }, []);

    const commitAndClose = useCallback(
        (applyNow?: () => void) => {
            pendingPredicatesRef.current = [];
            applyNow?.();
            commitAll();
            const predicates = pendingPredicatesRef.current;
            pendingPredicatesRef.current = [];
            if (predicates.length) onAddFilters(predicates);
            setIsOpen(false);
        },
        [commitAll, onAddFilters],
    );

    const filterMenu = useMemo(() => ({ registerCommit, commitAndClose }), [registerCommit, commitAndClose]);

    const onOpenChange = (open: boolean) => {
        if (open) setIsOpen(true);
        else commitAndClose();
    };

    const items: ItemType[] = useMemo(() => {
        return fields
            .filter((field) => field.field !== PARENT_DOCUMENT_FILTER_NAME || isContextDocumentsEnabled)
            .map((field) => {
                return {
                    key: field.field,
                    title: field.displayName,
                    type: 'item',
                    render: renderFilterMenuItem,
                    children: [
                        {
                            key: `${field.field}-children`,
                            type: 'custom',
                            render: () => (
                                <ValueMenu
                                    field={field}
                                    values={[]}
                                    defaultOptions={[]}
                                    includeCount={includeCount}
                                    onChangeValues={(values: any[]) => handleAddFilter(field, values)}
                                    isRenderedInSubMenu
                                />
                            ),
                        },
                    ],
                };
            });
    }, [fields, includeCount, handleAddFilter, isContextDocumentsEnabled]);

    return (
        <FilterMenuProvider value={filterMenu}>
            <Menu
                items={items}
                open={isOpen}
                onOpenChange={onOpenChange}
                overlayClassName={overlayClassName}
                overlayStyle={overlayStyle}
            >
                <TriggerAnchor ref={triggerRef}>
                    <AddFilterButton variant="text">
                        <Icon icon={Plus} size="md" />
                        {t('filters.addFilter')}
                    </AddFilterButton>
                </TriggerAnchor>
            </Menu>
        </FilterMenuProvider>
    );
}
