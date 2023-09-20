import Icon from '@ant-design/icons/lib/components/Icon';
import React from 'react';
import { ReactComponent as DomainsIcon } from '../../images/domain.svg';

type Props = {
    style?: React.CSSProperties;
};

export default function DomainIcon({ style }: Props) {
    return <Icon component={DomainsIcon} style={style} />;
}
