/* eslint-disable import/no-cycle */
import { Button, Icon, Menu } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ItemType } from '@components/components/Menu/types';

import { renderFilterMenuItem } from '@app/searchV2/filters/FilterMenuItem';
import { DEFAULT_FILTER_FIELDS } from '@app/searchV2/filters/field/fields';
import { FilterField, FilterPredicate } from '@app/searchV2/filters/types';
import { useFilterMenuOverlay } from '@app/searchV2/filters/useFilterMenuOverlay';
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
    onAddFilter: (predicate: FilterPredicate) => void;
    includeCount?: boolean;
}

export default function AddFilterDropdown({ fields = DEFAULT_FILTER_FIELDS, onAddFilter, includeCount }: Props) {
    const { t } = useTranslation('search');
    const isContextDocumentsEnabled = useIsContextDocumentsEnabled();
    const [isOpen, setIsOpen] = useState(false);
    const { triggerRef, overlayClassName, overlayStyle } = useFilterMenuOverlay<HTMLSpanElement>(isOpen);

    const handleAddFilter = useCallback(
        (field: FilterField, values: any[]) => {
            onAddFilter({
                field,
                operator: getDefaultFieldOperatorType(field),
                values,
                defaultValueOptions: [],
            });
            // The new filter's pill moves this button, and an open menu would jump with it.
            setIsOpen(false);
        },
        [onAddFilter],
    );

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
        <Menu
            items={items}
            open={isOpen}
            onOpenChange={setIsOpen}
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
    );
}
