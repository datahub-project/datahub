import { useState } from 'react';

type Props = {
    initialValue?: boolean;
    onRequestOpen?: () => void;
    onRequestClose?: () => void;
};

const NOOP = () => {};

const useToggle = ({ initialValue = false, onRequestOpen = NOOP, onRequestClose = NOOP }: Props) => {
    const [isOpen, setIsOpen] = useState<boolean>(initialValue);

    const toggle = () => {
        const requestOpen = !isOpen;
        if (requestOpen) onRequestOpen();
        else onRequestClose();
        setIsOpen(requestOpen);
    };

    return { isOpen, toggle } as const;
};

export default useToggle;
