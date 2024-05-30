import { ReactNode } from 'react';
import { message } from 'antd';

export enum ToastType {
    INFO = 'info',
    ERROR = 'error',
    SUCCESS = 'success',
    WARNING = 'warning',
}

export const showToastMessage = (type: ToastType, content: string | ReactNode, duration: number) => {
    message.open({
        type,
        content,
        duration,
    });
};
