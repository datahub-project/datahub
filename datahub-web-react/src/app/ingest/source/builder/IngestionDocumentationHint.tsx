import React from 'react';
import styled from 'styled-components';
import { Button, Tooltip } from 'antd';
import { CloseOutlined } from '@ant-design/icons';

import { SourceConfig } from './types';
import { ANTD_GRAY } from '../../../entity/shared/constants';

const Container = styled.div`
    background-color: #ffffff;
    border-radius: 8px;
    padding: 12px 12px 16px 24px;
    border: 1px solid #e0e0e0;
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
    color: ${ANTD_GRAY[6]};
`;

interface Props {
    sourceConfigs: SourceConfig;
    onHide: () => void;
}

export const IngestionDocumentationHint = ({ sourceConfigs, onHide }: Props) => {
    const { displayName, docsUrl } = sourceConfigs;
    return (
        <Container>
            <Header>
                <Title>Let&apos;s get connected! 🎉</Title>
                <Tooltip showArrow={false} title="Hide">
                    <Button type="text" icon={<StyledCloseOutlined />} onClick={onHide} />
                </Tooltip>
            </Header>
            <Description>
                <div style={{ marginBottom: 8 }}>
                    To import from {displayName}, we&apos;ll need some more information to connect to your instance.
                </div>
                <div>
                    Check out the{' '}
                    <a href={docsUrl} target="_blank" rel="noopener noreferrer">
                        {displayName} Guide
                    </a>{' '}
                    to understand the prerequisites, learn about available settings, and view examples to help connect
                    to the data source.
                </div>
            </Description>
        </Container>
    );
};
