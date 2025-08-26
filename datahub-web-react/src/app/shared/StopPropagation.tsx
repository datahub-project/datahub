import React, { PropsWithChildren } from 'react';

/**
 * Unstyled Component that prevents event propagation. Useful for preventing clicks from propagating to parent elements.
 */
export const StopPropagation = (props: PropsWithChildren<any>) => (
    <span
        aria-hidden="true"
        onClick={(e) => {
            e.preventDefault();
            e.stopPropagation();
        }}
        {...props}
    />
);
