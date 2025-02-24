import React from 'react';
import Icon from '@ant-design/icons';
import { CustomIconComponentProps } from '@ant-design/icons/lib/components/Icon';

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

export const CodeBlockIcon = (props: Partial<CustomIconComponentProps>) => (
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
                    d="M6.448 1.916a.525.525 0 0 0-.996-.332l-1.75 5.25a.525.525 0 1 0 .996.332l1.75-5.25Zm4.227.185a.525.525 0 1 0 0 1.05h1.4a.175.175 0 0 1 .175.175v8.05a.175.175 0 0 1-.175.175h-9.1a.175.175 0 0 1-.175-.175v-2.45a.525.525 0 0 0-1.05 0v2.45c0 .677.549 1.225 1.225 1.225h9.1a1.225 1.225 0 0 0 1.225-1.225v-8.05a1.225 1.225 0 0 0-1.225-1.225h-1.4Zm-2.583.35a.525.525 0 1 0-.784.698l1.09 1.227L7.307 5.6a.525.525 0 1 0 .784.698l1.4-1.574a.525.525 0 0 0 0-.697l-1.4-1.577ZM2.799 6.342a.525.525 0 0 1-.74-.043l-1.4-1.574a.525.525 0 0 1 0-.697l1.4-1.577a.525.525 0 1 1 .784.698l-1.09 1.227L2.843 5.6a.525.525 0 0 1-.044.742v-.001Z"
                />
            </svg>
        )}
    />
);
