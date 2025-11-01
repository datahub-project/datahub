import { useState, useCallback } from 'react';

/**
 * Generic modal state management hook
 * 
 * Handles common modal patterns: open, close, and data passing.
 * Reduces boilerplate for modal state management.
 * 
 * @template T The type of data associated with the modal
 * 
 * @example
 * ```tsx
 * const diffModal = useModal<Entity>();
 * 
 * // Open modal with data
 * <Button onClick={() => diffModal.open(entity)}>Show Diff</Button>
 * 
 * // Use in modal component
 * <DiffModal 
 *     isVisible={diffModal.isOpen}
 *     entity={diffModal.data}
 *     onClose={diffModal.close}
 * />
 * ```
 */
export function useModal<T = unknown>() {
    const [isOpen, setIsOpen] = useState(false);
    const [data, setData] = useState<T | null>(null);

    const open = useCallback((modalData?: T) => {
        if (modalData !== undefined) {
            setData(modalData);
        }
        setIsOpen(true);
    }, []);

    const close = useCallback(() => {
        setIsOpen(false);
        // Clear data after animation completes (typical modal transition time)
        setTimeout(() => setData(null), 300);
    }, []);

    const toggle = useCallback((modalData?: T) => {
        if (isOpen) {
            close();
        } else {
            open(modalData);
        }
    }, [isOpen, close, open]);

    return {
        isOpen,
        data,
        open,
        close,
        toggle,
    };
}

/**
 * Return type for useModal hook
 */
export type UseModalReturn<T> = ReturnType<typeof useModal<T>>;

