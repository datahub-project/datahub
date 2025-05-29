import { useEffect } from 'react';

import { checkIfMac } from '@app/utils/checkIfMac';

export const useCommandS = (onPress: () => void) => {
    useEffect(() => {
        const handleKeyDown = (event: KeyboardEvent) => {
            const isMac = checkIfMac();

            if ((event.metaKey || (!isMac && event.ctrlKey)) && event.key === 's') {
                event.preventDefault();
                onPress();
            }
        };
        window.addEventListener('keydown', handleKeyDown);
        return () => {
            window.removeEventListener('keydown', handleKeyDown);
        };
    }, [onPress]);
};
