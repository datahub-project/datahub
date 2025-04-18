import { useCallback, useEffect, useState } from 'react';
import { useRemirrorContext } from '@remirror/react';
import { ActionKind, AutocompleteAction, FromTo } from 'prosemirror-autocomplete';
import { DataHubMentionsExtension } from './DataHubMentionsExtension';

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
    const [index, selectedIndex] = useState(0);
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
                case ActionKind.enter:
                    return onEnter?.(items[index]) || true;
                case ActionKind.up:
                    selectedIndex((i) => (i - 1 < 0 ? maxIndex : i - 1));
                    return true;
                case ActionKind.down:
                    selectedIndex((i) => (i + 1 > maxIndex ? 0 : i + 1));
                    return true;
                default:
                    return false;
            }
        },
        [items, onEnter, selectedIndex, index],
    );

    useEffect(() => {
        const eventHandler = ext.addHandler('handleEvents', handleEvents);
        return () => eventHandler();
    }, [ext, handleEvents]);

    return { ...state, selectedIndex: index };
}
