import { Tooltip } from '@components';
import { ClipboardText } from '@phosphor-icons/react/dist/csr/ClipboardText';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

const DataContractLogo = styled(ClipboardText)`
    margin-left: 8px;
    font-size: 16px;
    color: ${(props) => props.theme.colors.iconBrand};
`;

export const DataContractBadge = ({ link }: { link: string }) => {
    const { t } = useTranslation('entity.profile.validations');
    const theme = useTheme();
    return (
        <Tooltip
            title={
                <Trans
                    t={t}
                    i18nKey="contractBadge.partOfContract"
                    components={{ anchor: <Link to={link} style={{ color: theme.colors.icon }} /> }}
                />
            }
        >
            <Link to={link}>
                <DataContractLogo />
            </Link>
        </Tooltip>
    );
};
