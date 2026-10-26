import { useEffect, useState } from 'react';

/*
 * Returns whether a desired component is rendered or not.
 * By setting a time out we place the state update at the
 * end of the queue after this component has rendered.
 */
export default function useHasComponentRendered() {
    const [hasRendered, setHasRendered] = useState(false);

    useEffect(() => {
        setTimeout(() => {
            setHasRendered(true);
        }, 0);
    }, []);

    return { hasRendered };
}
