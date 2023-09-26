/* eslint-disable @typescript-eslint/no-unused-vars */
import React from 'react';
import { Button, Typography } from 'antd';
import styled from 'styled-components';
import { ArrowRightOutlined } from '@ant-design/icons';
import { AssertionInfo } from '../../../../../../types.generated';

type Props = {
    assertionInfo: AssertionInfo;
    onViewAssertionDetails: () => void;
};

const StyledArrowRightOutlined = styled(ArrowRightOutlined)`
    font-size: 12px;
`;

/**
 * A human-readable description of a SQL Assertion.
 */
export const SqlAssertionDescription = ({ assertionInfo, onViewAssertionDetails }: Props) => {
    const { description } = assertionInfo;

    return (
        <div>
            <Typography.Text>{description}</Typography.Text>
            <Button
                type="link"
                onClick={(e) => {
                    e.stopPropagation();
                    onViewAssertionDetails();
                }}
            >
                Details <StyledArrowRightOutlined />
            </Button>
        </div>
    );
};
