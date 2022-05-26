import React from 'react';
import { BookOutlined } from '@ant-design/icons';

type Props = {
    name: string;
    style;
};

export default function TermPill({ name, style }: Props) {
    return (
        <div>
            <BookOutlined />
            <span style={style}>{name}</span>
        </div>
    );
}
