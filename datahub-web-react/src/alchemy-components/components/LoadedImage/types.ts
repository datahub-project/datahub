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
