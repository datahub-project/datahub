import { useState } from 'react';

const useToggle = () => {
    const [isOpen, setIsOpen] = useState<boolean>(false);

    const toggle = () => {
        const requestOpen = !isOpen;
        setIsOpen(requestOpen);
    };

    return { isOpen, toggle } as const;
};

export default useToggle;
