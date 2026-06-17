import { Typography } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components/macro';

const StyledParagraph = styled(Typography.Paragraph)`
    text-align: justify;
    text-justify: inter-word;
    margin: 40px 0;
    font-size: 15px;
`;

function EmptyDomainDescription() {
    const theme = useTheme();
    const { t } = useTranslation('governance.domain');
    return (
        <>
            <StyledParagraph type="secondary">
                <Trans
                    t={t}
                    i18nKey="empty.welcomeParagraph"
                    components={{ bold: <strong style={{ color: theme.colors.textSecondary }} /> }}
                />
            </StyledParagraph>
            <StyledParagraph type="secondary">
                <Trans
                    t={t}
                    i18nKey="empty.nestedParagraph"
                    components={{ bold: <strong style={{ color: theme.colors.textSecondary }} /> }}
                />
            </StyledParagraph>
            <StyledParagraph type="secondary">
                <Trans
                    t={t}
                    i18nKey="empty.dataProductsParagraph"
                    components={{ bold: <strong style={{ color: theme.colors.textSecondary }} /> }}
                />
            </StyledParagraph>
            <StyledParagraph type="secondary">{t('empty.ctaParagraph')}</StyledParagraph>
        </>
    );
}

export default EmptyDomainDescription;
