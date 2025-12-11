/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect } from 'react';

export const useEnterKeyListener = ({ querySelectorToExecuteClick }) => {
    const element = document?.querySelector(querySelectorToExecuteClick);

    useEffect(() => {
        const handlePressEnter = () => {
            const mouseClickEvents = ['mousedown', 'click', 'mouseup'];
            function simulateMouseClick(event) {
                mouseClickEvents?.forEach((mouseEventType) =>
                    event?.dispatchEvent(
                        new MouseEvent(mouseEventType, {
                            view: window,
                            bubbles: true,
                            cancelable: true,
                            buttons: 1,
                        }),
                    ),
                );
            }
            simulateMouseClick(element);
        };

        const listener = (event) => {
            if (event?.code === 'Enter' || event?.code === 'NumpadEnter') {
                handlePressEnter();
            }
        };

        document?.addEventListener('keydown', listener);

        return () => {
            document?.removeEventListener('keydown', listener);
        };
    });
};
