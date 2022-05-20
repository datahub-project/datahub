import React from 'react';
import { Tag } from 'antd';

type Props = {
    closable: boolean;
    onClose: (event?: React.MouseEvent<HTMLElement, MouseEvent> | undefined) => void;
    label: React.ReactNode;
};

export default function SelectedOwnerTag({ closable, onClose, label }: Props) {
    const onPreventMouseDown = (event) => {
        event.preventDefault();
        event.stopPropagation();
    };
    return (
        <Tag
            onMouseDown={onPreventMouseDown}
            closable={closable}
            onClose={onClose}
            style={{
                padding: '0px 7px 0px 0px',
                marginRight: 3,
                borderRadius: '100em',
                lineHeight: 0,
                display: 'flex',
                justifyContent: 'start',
                alignItems: 'center',
            }}
        >
            {label}
        </Tag>
    );
}
