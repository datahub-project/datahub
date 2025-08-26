import { useEffect, useState } from 'react';

type Props = {
    condition: boolean;
    delay?: number;
};

const NOOP = () => {};

const useDelayedKey = ({ condition, delay = 250 }: Props) => {
    const [key, setKey] = useState(1);

    useEffect(() => {
        if (!condition) return NOOP;

        const timer = window.setTimeout(() => setKey((k) => k + 1), delay);

        return () => {
            window.clearTimeout(timer);
        };
    }, [condition, delay]);

    return key;
};

export default useDelayedKey;
