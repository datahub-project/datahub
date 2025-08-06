import { useEffect } from 'react';

import { ClickOutsideCallback, ClickOutsideOptions } from '@components/components/Utils/ClickOutside/types';

export default function useClickOutside(callback: ClickOutsideCallback, options: ClickOutsideOptions) {
    useEffect(() => {
        /**
         * Handles click events outside the wrapper or based on selectors.
         */
        const handleClickOutside = (event: MouseEvent): void => {
            const target = event.target as HTMLElement;

            const { wrappers, ignoreSelector, ignoreWrapper, outsideSelector } = options;

            const isInsideOfWrappers = wrappers
                ? wrappers.some(
                      (wrapper) => wrapper.current && wrapper.current.contains((event.target as Node) || null),
                  )
                : false;

            // Ignore clicks on elements matching `ignoreSelector`
            if (ignoreSelector && target.closest(ignoreSelector)) {
                return;
            }

            // Trigger `onClickOutside` if the click is on an element matching `outsideSelector`
            if (outsideSelector && target.closest(outsideSelector)) {
                callback(event);
                return;
            }

            // Trigger `onClickOutside` if the click is outside the wrapper
            if (!ignoreWrapper && !isInsideOfWrappers) {
                callback(event);
            }
        };

        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, [callback, options]);
}
