import { useEffect, useState } from 'react';
import refractor from 'refractor/core';

import { loadHighlightLanguage } from '@components/components/CodeBlock/highlighter';

/**
 * Fetches the grammar for `language` when it is not registered yet and
 * re-renders once it is, so ingested view languages like scala still highlight.
 *
 * @param language - Canonical Prism language id
 * @returns True when the grammar is registered
 */
export function useHighlightLanguage(language: string): boolean {
    const isRegistered = refractor.registered(language);
    const [, setLoadedCount] = useState(0);

    useEffect(() => {
        if (isRegistered) {
            return undefined;
        }
        let isActive = true;
        loadHighlightLanguage(language).then((loaded) => {
            if (isActive && loaded) {
                setLoadedCount((count) => count + 1);
            }
        });
        return () => {
            isActive = false;
        };
    }, [isRegistered, language]);

    return isRegistered;
}
