import Icon from '@ant-design/icons/lib/components/Icon';
import React from 'react';
import DomainsIcon from '../../images/domain.svg?react';

type Props = {
    style?: React.CSSProperties;
};

export default function DomainIcon({ style }: Props) {
    return <Icon component={DomainsIcon} style={style} />;
}
