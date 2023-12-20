import { useGlossaryEntityData } from '../entity/shared/GlossaryEntityContext';
import useToggle from '../shared/useToggle';

const useToggleSidebar = () => {
    const { isSidebarOpen, setIsSidebarOpen } = useGlossaryEntityData();

    const { isOpen, toggle: toggleSidebar } = useToggle({
        initialValue: isSidebarOpen ?? true,
        onToggle: (isNowOpen: boolean) => {
            setIsSidebarOpen(isNowOpen);
        },
    });

    return { isOpen, toggleSidebar } as const;
};

export default useToggleSidebar;
