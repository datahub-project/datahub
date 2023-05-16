import { LoadingOutlined } from '@ant-design/icons';
import React, { useEffect, useRef, useState } from 'react';

type Props = {
    delay?: number;
};

const DelayedLoading = ({ delay = 250 }: Props) => {
    const timerRef = useRef(-1);
    const [isVisible, setIsVisible] = useState(false);
    useEffect(() => {
        timerRef.current = window.setTimeout(() => setIsVisible(true), delay);
        return () => window.clearTimeout(timerRef.current);
    }, [delay]);
    return isVisible ? <LoadingOutlined /> : null;
};

export default DelayedLoading;
