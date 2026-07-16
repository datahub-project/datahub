import { useEffect, useState } from 'react';

export default function useDebounceFalse(delay: number, ...bools: boolean[]) {
    const [isTrue, setIsTrue] = useState<boolean>(false);

    useEffect(() => {
        const anyTrue = bools.some(Boolean);

        if (anyTrue) {
            setIsTrue(true);
        } else {
            const timer = setTimeout(() => {
                setIsTrue(false);
            }, delay);

            return () => clearTimeout(timer);
        }
        return () => {};
    }, [bools, delay]);

    return isTrue;
}
