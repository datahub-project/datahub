/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
