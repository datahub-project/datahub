import { Tooltip } from '@components';
import { Button, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

const Container = styled(Button)<{ enabled }>`
    margin-bottom: 12px;
    padding: 20px 40px;
    height: 160px;
    display: flex;
    justify-content: center;
    border-radius: 16px;
    align-items: start;
    flex-direction: column;
    border: 1px solid ${ANTD_GRAY[4]};
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    ${(props) =>
        props.enabled &&
        `&&:hover {
        box-shadow: ${props.theme.styles['box-shadow-hover']};
    }`}
    && {
        text-align: start;
    }
    white-space: unset;
    color: ${(props) => (props.enabled ? 'normal' : ANTD_GRAY[6])};
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    margin-bottom: 12px;
`;

const Title = styled(Typography.Title)<{ enabled }>`
    && {
        padding: 0px;
        margin: 0px;
        color: ${(props) => (props.enabled ? 'normal' : ANTD_GRAY[6])};
    }
    margin-right: 8px;
`;

const Description = styled(Typography.Paragraph)`
    font-weight: normal;
    font-size: 14px;
`;

interface TypeOptionProps {
    name: string;
    description: string;
    icon?: React.ReactNode | null;
    enabled?: boolean;
    onClick: () => void;
}

/**
 * A specific Assertion Type option.
 */
export function AssertionTypeOption({ name, description, icon, enabled = true, onClick }: TypeOptionProps) {
    const handleOnClick = () => {
        if (enabled) {
            onClick();
        }
    };

    return (
        <Tooltip title={!enabled ? 'This option is not available for this data asset.' : undefined}>
            <Container onClick={handleOnClick} enabled={enabled} key={name}>
                <Header>
                    {icon}
                    <Title level={4} enabled={enabled}>
                        {name}
                    </Title>
                </Header>
                <Description type="secondary">{description}</Description>
            </Container>
        </Tooltip>
    );
}
