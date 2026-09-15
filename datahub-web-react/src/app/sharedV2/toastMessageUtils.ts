import { message } from 'antd';
import { ReactNode } from 'react';

export enum ToastType {
    ERROR = 'error',
    SUCCESS = 'success',
    LOADING = 'loading',
}

export const showToastMessage = (type: ToastType, content: string | ReactNode, duration: number) => {
    message.open({
        type,
        content,
        duration,
    });
};
