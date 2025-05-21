import { useEffect } from 'react';

export const useCommandS = (onPress: () => void) => {
    useEffect(() => {
        const handleKeyDown = (event: KeyboardEvent) => {
            if (event.metaKey && event.key === 's') {
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
