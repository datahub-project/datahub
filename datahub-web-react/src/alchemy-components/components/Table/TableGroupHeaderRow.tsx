import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { Checkbox } from '@components/components/Checkbox';
import {
    CheckboxWrapper,
    GroupCaretButton,
    GroupCaretSlot,
    GroupHeaderActions,
    GroupHeaderCell,
    GroupHeaderContent,
    GroupHeaderInner,
    GroupHeaderLabel,
    GroupHeaderRow,
} from '@components/components/Table/components';
import { TableGroupByProps, TableGroupRenderProps } from '@components/components/Table/types';
import { Text } from '@components/components/Text';

interface GroupSelectionState {
    isChecked: boolean;
    isIntermediate: boolean;
    isDisabled: boolean;
}

interface Props<T> {
    group: TableGroupRenderProps<T>;
    groupBy: TableGroupByProps<T>;
    columnCount: number;
    /** Set when the table has a selection column; the group checkbox sits in that column. */
    selection?: GroupSelectionState & { onSelect: () => void };
    onToggle: () => void;
    stickyTop?: number;
    /** Fixed height for virtualized tables, where every item must be the same height. */
    rowHeight?: number;
    /** Left padding of the first column when it is wider than the default, so the label stays aligned. */
    gutter?: string;
}

const stopPropagation = (e: React.MouseEvent) => e.stopPropagation();

export const TableGroupHeaderRow = <T,>({
    group,
    groupBy,
    columnCount,
    selection,
    onToggle,
    stickyTop,
    rowHeight,
    gutter,
}: Props<T>) => {
    const { t } = useTranslation('alchemy');
    const { renderGroupHeader, renderGroupActions, showCount = true, stickyGroupHeaders } = groupBy;
    const actions = renderGroupActions?.(group);

    return (
        <GroupHeaderRow onClick={onToggle} rowHeight={rowHeight} data-testid={groupBy.groupHeaderDataTestId?.(group)}>
            {selection && (
                <GroupHeaderCell isSticky={stickyGroupHeaders} stickyTop={stickyTop} onClick={stopPropagation}>
                    <CheckboxWrapper>
                        <Checkbox
                            isChecked={selection.isChecked}
                            isIntermediate={selection.isIntermediate}
                            isDisabled={selection.isDisabled}
                            onCheckboxChange={selection.onSelect}
                            aria-label={t('table.group.selectRows')}
                            size="xs"
                        />
                    </CheckboxWrapper>
                </GroupHeaderCell>
            )}
            <GroupHeaderCell
                colSpan={selection ? columnCount - 1 : columnCount}
                isSticky={stickyGroupHeaders}
                stickyTop={stickyTop}
                gutter={gutter}
            >
                <GroupHeaderInner>
                    <GroupHeaderContent gutter={gutter}>
                        <GroupCaretSlot>
                            <GroupCaretButton
                                type="button"
                                variant="text"
                                color="gray"
                                size="xs"
                                isCircle
                                icon={{ icon: group.collapsed ? CaretRight : CaretDown, color: 'icon' }}
                                aria-expanded={!group.collapsed}
                                aria-label={group.collapsed ? t('table.group.expand') : t('table.group.collapse')}
                                onClick={(e) => {
                                    e.stopPropagation();
                                    onToggle();
                                }}
                            />
                        </GroupCaretSlot>
                        <GroupHeaderLabel>{renderGroupHeader(group)}</GroupHeaderLabel>
                        {showCount && (
                            <Text type="span" size="sm" color="textTertiary">
                                {group.rows.length}
                            </Text>
                        )}
                    </GroupHeaderContent>
                    {actions && <GroupHeaderActions onClick={stopPropagation}>{actions}</GroupHeaderActions>}
                </GroupHeaderInner>
            </GroupHeaderCell>
        </GroupHeaderRow>
    );
};
