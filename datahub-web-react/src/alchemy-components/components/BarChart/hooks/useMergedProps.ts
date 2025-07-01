import { useMemo } from 'react';

export default function useMergedProps<PropsType extends object>(
    props?: PropsType,
    defaultProps?: PropsType,
): PropsType {
    return useMemo(() => ({ ...(defaultProps || {}), ...(props || {}) }), [props, defaultProps]) as PropsType;
}
