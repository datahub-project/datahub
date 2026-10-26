import { useEffect } from 'react';

export default function useAutoFocusInModal(ref: React.MutableRefObject<any>) {
    useEffect(() => {
        if (ref && ref.current) {
            ref.current.focus();
        }
    }, [ref]);
}
