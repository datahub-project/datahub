import { useMemo } from 'react';

import { useModalContext } from '@components/components/Modal/ModalContext';

export const useGetModalLinkProps = () => {
    const { isInsideModal } = useModalContext();

    return useMemo(() => {
        if (isInsideModal) {
            return { target: '_blank', rel: 'noopener noreferrer' };
        }
        return {};
    }, [isInsideModal]);
};
