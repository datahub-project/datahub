import { useState } from 'react';

const useToggle = (initialValue = false) => {
    const [isOpen, setIsOpen] = useState<boolean>(initialValue);

    const toggle = () => {
        const requestOpen = !isOpen;
        setIsOpen(requestOpen);
    };

    return { isOpen, toggle } as const;
};

export default useToggle;
