/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { PlusOutlined } from '@ant-design/icons';
import { Button, Empty, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

const StyledEmpty = styled(Empty)`
    padding: 80px 40px;
    .ant-empty-footer {
        .ant-btn:not(:last-child) {
            margin-right: 8px;
        }
    }
`;

const StyledButton = styled(Button)`
    margin-right: 8px;
`;

interface Props {
    title?: string;
    description?: string;
    onAddTerm: () => void;
    onAddtermGroup: () => void;
}

function EmptyGlossarySection(props: Props) {
    const { title, description, onAddTerm, onAddtermGroup } = props;

    return (
        <>
            <StyledEmpty
                description={
                    <>
                        <Typography.Title level={4}>{title}</Typography.Title>
                        <Typography.Paragraph type="secondary">{description}</Typography.Paragraph>
                    </>
                }
            >
                {/* not disabled on acryl-main due to ability to propose */}
                <StyledButton data-testid="add-term-button" onClick={onAddTerm}>
                    <PlusOutlined /> Add Term
                </StyledButton>
                <StyledButton data-testid="add-term-group-button" onClick={onAddtermGroup}>
                    <PlusOutlined /> Add Term Group
                </StyledButton>
            </StyledEmpty>
        </>
    );
}

export default EmptyGlossarySection;
