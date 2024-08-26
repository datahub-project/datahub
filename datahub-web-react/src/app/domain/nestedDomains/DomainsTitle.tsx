import React from 'react';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';
import DomainIcon from '../DomainIcon';

const IconWrapper = styled.span`
    margin-right: 10px;
`;

export default function DomainsTitle() {
    const { t } = useTranslation();
    return (
        <span>
            <IconWrapper>
                <DomainIcon />
            </IconWrapper>
            {t('common.domains')}
        </span>
    );
}
