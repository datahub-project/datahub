import { EditOutlined, ExpandAltOutlined, FileOutlined } from '@ant-design/icons';
import { Divider, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useEntityData, useRefetch, useRouteToTab } from '@app/entity/shared/EntityContext';
import { AddLinkModal } from '@app/entityV2/shared/components/styled/AddLinkModal';
import { EmptyTab } from '@app/entityV2/shared/components/styled/EmptyTab';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { LinkList } from '@app/entityV2/shared/tabs/Documentation/components/LinkList';
import { Editor } from '@app/entityV2/shared/tabs/Documentation/components/editor/Editor';
import { Button } from '@src/alchemy-components';

const Header = styled.div`
    display: flex;
    align-items: start;
    justify-content: space-between;
    padding: 16px 4px;
`;

const Title = styled(Typography.Title)`
    && {
        color: ${ANTD_GRAY[9]};
        padding: 0px;
        margin: 0px;
        display: flex;
        align-items: center;
    }
`;

const ThinDivider = styled(Divider)`
    && {
        padding-top: 0px;
        padding-bottom: 0px;
        margin-top: 0px;
        margin-bottom: 20px;
    }
`;

const Documentation = styled.div`
    .remirror-editor.ProseMirror {
        padding: 0px 8px;
    }
`;

const StyledFileOutlined = styled(FileOutlined)`
    && {
        font-size: 16px;
        margin-right: 8px;
    }
`;

export const DocumentationSection = () => {
    // The summary tab consists of modules
    const { entityData } = useEntityData();
    const refetch = useRefetch();
    const routeToTab = useRouteToTab();

    const description = entityData?.editableProperties?.description || entityData?.properties?.description || '';
    const hasDescription = description || description !== '';

    return (
        <>
            <Header>
                <Title level={3}>
                    <StyledFileOutlined />
                    About
                </Title>
                {hasDescription && (
                    <Button
                        variant="text"
                        onClick={() =>
                            routeToTab({
                                tabName: 'Documentation',
                                tabParams: { modal: true },
                            })
                        }
                    >
                        <ExpandAltOutlined />
                    </Button>
                )}
            </Header>
            <ThinDivider />
            <Documentation>
                {(hasDescription && <Editor content={description} readOnly />) || (
                    <EmptyTab tab="documentation">
                        <AddLinkModal refetch={refetch} />
                        <Button
                            data-testid="add-documentation"
                            onClick={() => routeToTab({ tabName: 'Documentation', tabParams: { editing: true } })}
                        >
                            <EditOutlined /> Add Documentation
                        </Button>
                    </EmptyTab>
                )}
                <LinkList refetch={refetch} />
            </Documentation>
        </>
    );
};
