import { HTMLAttributes } from 'react';

export type DividerType = 'horizontal' | 'vertical';

export type DividerProps = HTMLAttributes<HTMLDivElement> & {
    type?: DividerType;
    dashed?: boolean;
};
