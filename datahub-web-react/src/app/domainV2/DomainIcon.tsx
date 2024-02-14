import Icon from '@ant-design/icons/lib/components/Icon';
import React from 'react';
import DomainsIcon from '../../images/domain.svg?react';
import { TYPE_ICON_CLASS_NAME } from '../entityV2/shared/components/subtypes';

type Props = {
    style?: React.CSSProperties;
};

export default function DomainIcon({ style }: Props) {
    return <Icon component={DomainsIcon} className={TYPE_ICON_CLASS_NAME} style={style} />;
}
