import { toast } from '@components';
import { useEffect, useRef } from 'react';

/**
 * Shows a persistent loading toast while `isLoading` is true and removes it
 * when loading finishes or the component unmounts.
 */
export function useLoadingToast(isLoading: boolean, content: string) {
    const keyRef = useRef(`loading-toast-${Math.random().toString(36).slice(2, 9)}`);

    useEffect(() => {
        if (!isLoading) {
            return undefined;
        }
        const key = keyRef.current;
        toast.loading(content, { key, duration: 0 });
        return () => toast.destroy(key);
    }, [isLoading, content]);
}
