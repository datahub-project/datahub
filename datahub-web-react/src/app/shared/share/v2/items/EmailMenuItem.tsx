import { CheckOutlined, MailOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import qs from 'query-string';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { StyledMenuItem } from '@app/shared/share/v2/styledComponents';

interface EmailMenuItemProps {
    urn: string;
    name: string;
    type: string;
    key: string;
}

const TextSpan = styled.span`
    padding-left: 12px;
    margin-left: 0px !important;
`;

export default function EmailMenuItem({ urn, name, type, key }: EmailMenuItemProps) {
    /**
     * Whether button has been clicked
     */
    const { t } = useTranslation('shared.share');
    const [isClicked, setIsClicked] = useState(false);
    const linkText = window.location.href;

    const link = qs.stringifyUrl({
        url: 'mailto:',
        query: {
            /* eslint-disable i18next/no-literal-string -- (untranslated-text) mailto subject/body sent outside the app */
            subject: `${name} | ${type}`,
            body: `Check out this ${type} on DataHub: ${linkText}. Urn: ${urn}`,
            /* eslint-enable */
        },
    });

    return (
        <StyledMenuItem
            key={key}
            onClick={() => {
                setIsClicked(true);
            }}
        >
            <Tooltip title={t('email.tooltip', { type })}>
                {isClicked ? <CheckOutlined /> : <MailOutlined />}
                <TextSpan>
                    <a href={link} target="_blank" rel="noreferrer" style={{ color: 'inherit' }}>
                        <b>{t('email.label')}</b>
                    </a>
                </TextSpan>
            </Tooltip>
        </StyledMenuItem>
    );
}
