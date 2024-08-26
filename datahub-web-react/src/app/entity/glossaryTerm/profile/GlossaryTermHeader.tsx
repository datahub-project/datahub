import { Divider, Space, Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { AvatarsGroup } from '../../../shared/avatar';

type Props = {
    definition: string;
    sourceRef: string;
    sourceUrl: string;
    ownership?: any;
};
export default function GlossaryTermHeader({ definition, sourceRef, sourceUrl, ownership }: Props) {
    const entityRegistry = useEntityRegistry();
    const { t } = useTranslation();
    return (
        <>
            <Space direction="vertical" size="middle" style={{ marginBottom: '15px' }}>
                <Typography.Paragraph>{definition}</Typography.Paragraph>
                <Space split={<Divider type="vertical" />}>
                    <Typography.Text>Source</Typography.Text>
                    <Typography.Text strong>{sourceRef}</Typography.Text>
                    {sourceUrl && (
                        <a href={decodeURIComponent(sourceUrl)} target="_blank" rel="noreferrer">
                            {t('settings.viewSource')}
                        </a>
                    )}
                </Space>
                <AvatarsGroup owners={ownership?.owners} entityRegistry={entityRegistry} />
            </Space>
        </>
    );
}
