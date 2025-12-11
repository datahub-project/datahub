/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ImgHTMLAttributes } from 'react';

export interface LoadedImageProps extends Omit<ImgHTMLAttributes<HTMLImageElement>, 'onLoad' | 'onError'> {
    src: string;
    alt: string;
    width?: string;
    errorMessage?: string;
    showErrorDetails?: boolean;
    onLoad?: () => void;
    onError?: () => void;
}
