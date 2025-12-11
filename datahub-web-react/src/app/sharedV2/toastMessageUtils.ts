/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
