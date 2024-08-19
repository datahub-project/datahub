import React from 'react';
import { Empty, Typography } from 'antd';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';

export type Props = {
    rawSchema: string | null;
};

const Content = styled.div`
    margin-left: 32px;
    flex-grow: 1;
`;

export default function SchemaView({ rawSchema }: Props) {
    const { t } = useTranslation();

    return (
        <>
            {rawSchema && rawSchema.length > 0 ? (
                <Typography.Text data-testid="schema-raw-view">
                    <pre>
                        <code>{rawSchema}</code>
                    </pre>
                </Typography.Text>
            ) : (
                <Content>
                    <Empty description={t('common.noSchema')} image={Empty.PRESENTED_IMAGE_SIMPLE} />
                </Content>
            )}
        </>
    );
}
