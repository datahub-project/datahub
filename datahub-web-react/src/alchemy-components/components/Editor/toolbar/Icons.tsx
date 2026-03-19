import Icon from '@ant-design/icons';
import { CustomIconComponentProps } from '@ant-design/icons/lib/components/Icon';
import React from 'react';

export const CodeIcon = (props: Partial<CustomIconComponentProps>) => (
    <Icon
        component={() => (
            <svg
                width="1em"
                height="1em"
                fill="currentColor"
                xmlns="http://www.w3.org/2000/svg"
                viewBox="0 0 14 14"
                {...props}
            >
                <path
                    fillRule="evenodd"
                    clipRule="evenodd"
                    d="M8.737 2.048a.526.526 0 0 0-1.022-.246l-2.45 10.15a.526.526 0 1 0 1.022.246l2.45-10.15ZM4.035 3.643a.525.525 0 0 0-.742.022L.493 6.64a.525.525 0 0 0 0 .72l2.8 2.975a.525.525 0 0 0 .764-.72L1.597 7l2.46-2.615a.525.525 0 0 0-.022-.742Zm5.93 0a.525.525 0 0 1 .742.022l2.8 2.975a.525.525 0 0 1 0 .72l-2.8 2.975a.525.525 0 0 1-.764-.72L12.405 7 9.943 4.385a.525.525 0 0 1 .023-.742Z"
                />
            </svg>
        )}
    />
);
