import { Globe } from '@phosphor-icons/react';
import React from 'react';

type Props = {
    style?: React.CSSProperties;
};

export default function DomainIcon({ style }: Props) {
    return <Globe style={style} />;
}
