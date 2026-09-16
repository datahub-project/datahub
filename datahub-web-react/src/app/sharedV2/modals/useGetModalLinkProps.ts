import { useMemo } from 'react';

import { useModalContext } from '@app/sharedV2/modals/ModalContext';

export const useGetModalLinkProps = () => {
    const { isInsideModal } = useModalContext();

    return useMemo(() => {
        if (isInsideModal) {
            return { target: '_blank', rel: 'noopener noreferrer' };
        }
        return {};
    }, [isInsideModal]);
};
