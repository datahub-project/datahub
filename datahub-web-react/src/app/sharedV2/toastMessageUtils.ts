import { toast } from '@components';
import { ReactNode } from 'react';

export enum ToastType {
    ERROR = 'error',
    SUCCESS = 'success',
    LOADING = 'loading',
}

export const showToastMessage = (type: ToastType, content: string | ReactNode, duration: number) => {
    toast[type](content, { duration });
};
