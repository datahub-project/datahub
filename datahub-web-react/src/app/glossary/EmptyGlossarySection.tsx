import { PlusOutlined } from '@ant-design/icons';
import { Button, Empty, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { EntityType } from '../../types.generated';
import CreateGlossaryEntityModal from '../entity/shared/EntityDropdown/CreateGlossaryEntityModal';

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
    refetchForTerms?: () => void;
    refetchForNodes?: () => void;
}

function EmptyGlossarySection(props: Props) {
    const { refetchForTerms, refetchForNodes } = props;

    const [isCreateTermModalVisible, setIsCreateTermModalVisible] = useState(false);
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);

    return (
        <>
            <StyledEmpty
                description={
                    <>
                        <Typography.Title level={4}>Empty Glossary</Typography.Title>
                        <Typography.Paragraph type="secondary">
                            Create Terms and Term Groups to organize data assets using a shared vocabulary.
                        </Typography.Paragraph>
                    </>
                }
            >
                <StyledButton onClick={() => setIsCreateTermModalVisible(true)}>
                    <PlusOutlined /> Add Term
                </StyledButton>
                <StyledButton onClick={() => setIsCreateNodeModalVisible(true)}>
                    <PlusOutlined /> Add Term Group
                </StyledButton>
            </StyledEmpty>
            {isCreateTermModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryTerm}
                    onClose={() => setIsCreateTermModalVisible(false)}
                    refetchData={refetchForTerms}
                />
            )}
            {isCreateNodeModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryNode}
                    onClose={() => setIsCreateNodeModalVisible(false)}
                    refetchData={refetchForNodes}
                />
            )}
        </>
    );
}

export default EmptyGlossarySection;
