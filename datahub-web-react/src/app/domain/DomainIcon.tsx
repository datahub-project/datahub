import React from 'react';
import { Globe } from '@phosphor-icons/react';

type Props = {
    style?: React.CSSProperties;
};

export default function DomainIcon({ style }: Props) {
    return <Globe style={style} />;
}
