import { useMemo, useState } from 'react';

import { ExpandableProps } from '@components/components/Table/types';
import { getExpandableRowKeys } from '@components/components/Table/utils';

type Args<T> = {
    data: T[];
    rowKey?: string | ((record: T) => string);
    expandable?: ExpandableProps<T>;
};

/**
 * Expansion state for both grouped rows (`expandedRowRender`) and tree data (`childrenColumnName`).
 * Precedence: controlled `expandedRowKeys`, then the legacy name-based `expandedGroupIds`, then
 * internal state seeded from `defaultExpandedRowKeys` / `defaultExpandAllRows`.
 */
export function useTableExpansion<T>({ data, rowKey, expandable }: Args<T>) {
    const controlledKeys = expandable?.expandedRowKeys;
    const legacyGroupIds = expandable?.expandedGroupIds;
    const isUncontrolled = !controlledKeys && !legacyGroupIds;

    const [internalKeys, setInternalKeys] = useState<Set<string>>(() => {
        if (expandable?.defaultExpandAllRows && expandable.childrenColumnName) {
            return new Set(getExpandableRowKeys(data, expandable.childrenColumnName, rowKey));
        }
        return new Set(expandable?.defaultExpandedRowKeys ?? []);
    });

    const expandedKeys = useMemo(
        () => (controlledKeys ? new Set(controlledKeys) : internalKeys),
        [controlledKeys, internalKeys],
    );

    const isRowExpanded = (record: T, key: string): boolean => {
        if (controlledKeys) return expandedKeys.has(key);
        if (legacyGroupIds) return legacyGroupIds.includes((record as { name?: string } | undefined)?.name ?? '');
        return internalKeys.has(key);
    };

    const toggleRow = (record: T, key: string) => {
        const expanded = !isRowExpanded(record, key);
        if (isUncontrolled) {
            setInternalKeys((previous) => {
                const next = new Set(previous);
                if (expanded) next.add(key);
                else next.delete(key);
                return next;
            });
        }
        expandable?.onExpand?.(expanded, record);
    };

    return { expandedKeys, isRowExpanded, toggleRow };
}
