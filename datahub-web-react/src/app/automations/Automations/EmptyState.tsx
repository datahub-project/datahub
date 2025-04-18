import { Icon } from '@components';
import { Empty, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';

const EmptyDomainContainer = styled.div`
    display: flex;
    justify-content: center;
    overflow-y: auto;
`;

const StyledEmpty = styled(Empty)`
    width: 35vw;
    margin: 0 auto;
    @media screen and (max-width: 1300px) {
        width: 50vw;
    }
    @media screen and (max-width: 896px) {
        overflow-y: auto;
        max-height: 75vh;
        &::-webkit-scrollbar {
            width: 5px;
            background: #d6d6d6;
        }
    }
    padding: 20px;
    .ant-empty-image {
        display: none;
    }
`;

const IconContainer = styled.div`
    display: flex;
    align-items: center;
    flex-direction: column;
    color: ${ANTD_GRAY[7]};
    font-size: 40px;
    margin-bottom: 8px;
`;

const StyledParagraph = styled(Typography.Paragraph)`
    text-align: center;
    text-justify: inter-word;
    margin: 20px 0;
    font-size: 15px;
`;

export const EmptyState = () => (
    <EmptyDomainContainer>
        <StyledEmpty
            description={
                <>
                    <IconContainer>
                        <Icon icon="AutoFixHigh" />
                    </IconContainer>
                    <Typography.Title level={4}>No Automations Yet!</Typography.Title>
                    <StyledParagraph type="secondary">
                        Create an Automation to streamline actions across your data assets.
                    </StyledParagraph>
                </>
            }
        />
    </EmptyDomainContainer>
);
