import { Tooltip } from '@components';
import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { Button, Typography } from 'antd';
import { CloseOutlined } from '@ant-design/icons';
import { purple } from '@ant-design/colors';
import { ANTD_GRAY } from '../../../entity/shared/constants';

const Container = styled.div`
    background-color: ${purple[0]};
    border-radius: 8px;
    padding: 12px 12px 16px 24px;
    border: 1px solid ${purple[1]};
    margin-bottom: 20px;
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

const Title = styled(Typography.Paragraph)`
    font-size: 16px;
    font-weight: bold;
    margin-bottom: 0px !important;
`;

const DescriptionContainer = styled.div`
    font-size: 14px;
    max-width: 90%;
`;
const Description = styled(Typography.Paragraph)`
    margin-bottom: 0.5em !important;
`;

const StyledCloseOutlined = styled(CloseOutlined)`
    color: ${ANTD_GRAY[6]};
`;

type Props = {
    visible: boolean;
};

export const OidcIntegrationHint = ({ visible }: Props) => {
    const [isVisible, setIsVisible] = useState(visible);

    useEffect(() => {
        setIsVisible(visible);
    }, [visible]);

    if (!isVisible) {
        return null;
    }

    return (
        <Container>
            <Header>
                <Title>Let&apos;s get connected! 🎉</Title>
                <Tooltip showArrow={false} title="Hide">
                    <Button type="text" icon={<StyledCloseOutlined />} onClick={() => setIsVisible(false)} />
                </Tooltip>
            </Header>
            <DescriptionContainer>
                <div>
                    <Description>
                        You’ll need to get a few details from your Identity Provider for{' '}
                        <a target="_blank" rel="noreferrer" href="https://www.pingidentity.com/en/openid-connect.html">
                            OpenID
                        </a>{' '}
                        to work. Don’t worry, it’s quick and easy.
                    </Description>
                    <div>
                        👉{' '}
                        <a
                            href="https://datahubproject.io/docs/managed-datahub/integrations/oidc-sso-integration/"
                            target="_blank"
                            rel="noopener noreferrer"
                        >
                            Click here to learn how
                        </a>
                        .
                    </div>
                </div>
            </DescriptionContainer>
        </Container>
    );
};
