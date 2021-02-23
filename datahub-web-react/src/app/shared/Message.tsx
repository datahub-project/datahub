import React, { ReactNode, useEffect, useMemo } from 'react';
import { message } from 'antd';

type MessageType = 'loading' | 'info' | 'error' | 'warning' | 'success';
export type MessageProps = {
    type: MessageType;
    content: ReactNode;
    style?: React.CSSProperties;
};

export const Message = ({ type, content, style }: MessageProps): JSX.Element => {
    const key = useMemo(() => {
        // We don't actually care about cryptographic security, but instead
        // just want something unique. That's why it's OK to use Math.random
        // here. See https://stackoverflow.com/a/8084248/5004662.
        return `message-${Math.random().toString(36).substring(7)}`;
    }, []);

    useEffect(() => {
        const hide = message.open({
            key,
            type,
            content,
            duration: 0,
            style,
        });
        return () => {
            hide();
        };
    }, [key, type, content, style]);

    return <></>;
};
