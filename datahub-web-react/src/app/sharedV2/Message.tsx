import { toast } from '@components';
import React, { ReactNode, useEffect, useMemo } from 'react';

type MessageType = 'loading' | 'info' | 'error' | 'warning' | 'success';
type MessageProps = {
    type: MessageType;
    content: ReactNode;
    /**
     * @deprecated Ignored. Retained so existing call sites keep compiling; alchemy toast
     * positions via placement, not arbitrary style.
     */
    style?: React.CSSProperties;
};

export const Message = ({ type, content, style: _style }: MessageProps): JSX.Element => {
    const key = useMemo(() => {
        // We don't actually care about cryptographic security, but instead
        // just want something unique. That's why it's OK to use Math.random
        // here. See https://stackoverflow.com/a/8084248/5004662.
        return `message-${Math.random().toString(36).substring(7)}`;
    }, []);

    useEffect(() => {
        toast[type](content, { key, duration: 0 });
        return () => {
            toast.destroy(key);
        };
    }, [key, type, content]);

    return <></>;
};
