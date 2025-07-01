import { message } from 'antd';
import { ReactNode } from 'react';

export enum ToastType {
    INFO = 'info',
    ERROR = 'error',
    SUCCESS = 'success',
    WARNING = 'warning',
    LOADING = 'loading',
}

export const showToastMessage = (type: ToastType, content: string | ReactNode, duration: number) => {
    message.open({
        type,
        content,
        duration,
    });
};
