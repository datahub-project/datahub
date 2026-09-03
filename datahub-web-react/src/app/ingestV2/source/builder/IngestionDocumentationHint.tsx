import { CloseOutlined } from '@ant-design/icons';
import { Button, Tooltip } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SourceConfig } from '@app/ingestV2/source/builder/types';

const Container = styled.div`
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 8px;
    padding: 12px 12px 16px 24px;
    border: 1px solid ${(props) => props.theme.colors.border};
    margin-bottom: 20px;
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 12px;
`;

const Title = styled.div`
    font-size: 16px;
    font-weight: bold;
`;

const Description = styled.div`
    font-size: 14px;
    max-width: 90%;
`;

const StyledCloseOutlined = styled(CloseOutlined)`
    color: ${(props) => props.theme.colors.textTertiary};
`;

interface Props {
    sourceConfigs: SourceConfig;
    onHide: () => void;
}

export const IngestionDocumentationHint = ({ sourceConfigs, onHide }: Props) => {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const { displayName, docsUrl } = sourceConfigs;
    return (
        <Container>
            <Header>
                <Title>{t('docHint.title')}</Title>
                <Tooltip showArrow={false} title={t('docHint.hideTooltip')}>
                    <Button type="text" icon={<StyledCloseOutlined />} onClick={onHide} />
                </Tooltip>
            </Header>
            <Description>
                <div style={{ marginBottom: 8 }}>{t('docHint.intro', { displayName })}</div>
                <div>
                    <Trans
                        t={t}
                        i18nKey="docHint.guide"
                        values={{ displayName }}
                        components={{
                            anchor: (
                                <a href={docsUrl} target="_blank" rel="noopener noreferrer">
                                    {t('docHint.guideLinkText', { displayName })}
                                </a>
                            ),
                        }}
                    />
                </div>
            </Description>
        </Container>
    );
};
