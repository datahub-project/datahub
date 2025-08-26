import { PlusOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Button, Empty, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { useUserContext } from '@app/context/useUserContext';

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
    const { platformPrivileges } = useUserContext();
    // Show create options only if the user has priviliges to manage or propose glossaries
    const showCreateTerm = platformPrivileges?.manageGlossaries || platformPrivileges?.proposeCreateGlossaryTerm;
    const showCreateTermGroup = platformPrivileges?.manageGlossaries || platformPrivileges?.proposeCreateGlossaryNode;

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
                <Tooltip
                    showArrow={false}
                    title={showCreateTerm ? '' : 'Reach out to your DataHub admin to add Glossary.'}
                    placement="left"
                >
                    <StyledButton data-testid="add-term-button" onClick={onAddTerm} disabled={!showCreateTerm}>
                        <PlusOutlined /> Add Term
                    </StyledButton>
                </Tooltip>
                <Tooltip
                    showArrow={false}
                    title={showCreateTermGroup ? '' : 'Reach out to your DataHub admin to add Glossary.'}
                    placement="left"
                >
                    <StyledButton
                        data-testid="add-term-group-button"
                        onClick={onAddtermGroup}
                        disabled={!showCreateTermGroup}
                    >
                        <PlusOutlined /> Add Term Group
                    </StyledButton>
                </Tooltip>
            </StyledEmpty>
        </>
    );
}

export default EmptyGlossarySection;
