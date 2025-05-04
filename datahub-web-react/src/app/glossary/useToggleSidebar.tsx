import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import useToggle from '@app/shared/useToggle';

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
