import React from 'react';

interface Props {
    children: React.ReactNode;
}

const StopPropagationWrapper = ({ children }: Props) => {
    return (
        <span
            onClick={(e) => e.stopPropagation()}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                    e.stopPropagation();
                }
            }}
        >
            {children}
        </span>
    );
};

export default StopPropagationWrapper;
