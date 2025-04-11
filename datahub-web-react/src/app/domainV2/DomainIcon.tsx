import React from 'react';
import { Globe } from '@phosphor-icons/react';
import { TYPE_ICON_CLASS_NAME } from '../entityV2/shared/components/subtypes';

type Props = {
    style?: React.CSSProperties;
};

export default function DomainIcon({ style }: Props) {
    return <Globe className={TYPE_ICON_CLASS_NAME} style={style} />;
}
