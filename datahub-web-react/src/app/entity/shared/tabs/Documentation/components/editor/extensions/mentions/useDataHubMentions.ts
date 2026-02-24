import { useRemirrorContext } from '@remirror/react';
import { ActionKind, AutocompleteAction, FromTo } from 'prosemirror-autocomplete';
import { useCallback, useEffect, useState } from 'react';

import { DataHubMentionsExtension } from '@app/entity/shared/tabs/Documentation/components/editor/extensions/mentions/DataHubMentionsExtension';

type State = {
    active: boolean;
    range?: FromTo;
    filter?: string;
};

type UseDataHubMentionsProps<T> = {
    items?: T[];
    onEnter?(item: T): boolean;
};

/**
 * This hook is a helper utility to read actions from prosemirror-autocomplete, allowing components
 * to react to these changes accordingly. It provides the postional and query metadata of the currently
 * active autocomplete menu. In addition, this hooks stores the selected index triggered
 * when using the arrow keys to navigate the autocomplete menu.
 */
export function useDataHubMentions<Item = any>(props: UseDataHubMentionsProps<Item>) {
    const [selectedIndex, setSelectedIndex] = useState(0);
    const rmrCtx = useRemirrorContext();
    const ext = rmrCtx.getExtension(DataHubMentionsExtension);
    const [state, setState] = useState<State>({ active: false });
    const { items, onEnter } = props;

    const handleEvents = useCallback(
        (action: AutocompleteAction) => {
            const active = !(action.kind === ActionKind.close || action.kind === ActionKind.enter);
            setState({ active, range: action.range, filter: action.filter });

            if (!items) {
                return false;
            }

            const maxIndex = items.length - 1;

            switch (action.kind) {
                case ActionKind.enter: {
                    // Guard: items may be empty during debounce window after dropdown renders
                    const selectedItem = items[selectedIndex];
                    if (!selectedItem) return false;
                    return onEnter?.(selectedItem) || true;
                }
                case ActionKind.up:
                    setSelectedIndex((i) => (i - 1 < 0 ? maxIndex : i - 1));
                    return true;
                case ActionKind.down:
                    setSelectedIndex((i) => (i + 1 > maxIndex ? 0 : i + 1));
                    return true;
                default:
                    return false;
            }
        },
        [items, onEnter, setSelectedIndex, selectedIndex],
    );

    useEffect(() => {
        const eventHandler = ext.addHandler('handleEvents', handleEvents);
        return () => eventHandler();
    }, [ext, handleEvents]);

    // Reset selectedIndex when items change to prevent out-of-bounds access
    useEffect(() => {
        setSelectedIndex(0);
    }, [items]);

    return { ...state, selectedIndex };
}
