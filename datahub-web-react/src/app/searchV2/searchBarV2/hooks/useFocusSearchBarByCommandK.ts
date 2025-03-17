import React, { useEffect } from 'react';

export default function useFocusElementByCommandK(ref: React.RefObject<HTMLElement>, disable?: boolean) {
    useEffect(() => {
        if (!disable) {
            const handleKeyDown = (event: KeyboardEvent) => {
                const isMac = (navigator as any).userAgentData
                    ? (navigator as any).userAgentData.platform.toLowerCase().includes('mac')
                    : navigator.userAgent.toLowerCase().includes('mac');

                // Support command-k to select the search bar on all platforms
                // Support ctrl-k to select the search bar on non-Mac platforms
                // 75 is the keyCode for 'k'
                if ((event.metaKey || (!isMac && event.ctrlKey)) && event.keyCode === 75) {
                    ref.current?.focus();
                }
            };
            document.addEventListener('keydown', handleKeyDown);
            return () => {
                document.removeEventListener('keydown', handleKeyDown);
            };
        }
        return () => null;
    }, [disable, ref]);
}
