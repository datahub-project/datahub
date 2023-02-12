import React, { useState } from 'react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { DeleteOutlined, EditOutlined, ExpandOutlined, MoreOutlined } from '@ant-design/icons';
import { Button, Dropdown, Menu, message, Modal, Typography } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';
import { CorpUser } from '../../../../../../types.generated';
import QueryModal from './QueryModal';
import CopyQuery from './CopyQuery';
import { useDeleteQueryMutation } from '../../../../../../graphql/query.generated';
import { toLocalDateString } from '../../../../../shared/time/timeUtils';
import QueryBuilderModal from './QueryBuilderModal';
import NoMarkdownViewer from '../../../components/styled/StripMarkdownText';

export type Props = {
    urn?: string;
    query?: string | null;
    title?: string;
    description?: string;
    createdAtMs?: number;
    createdBy?: CorpUser;
    showDelete?: boolean;
    showEdit?: boolean;
    filterText?: string;
    onDeleted?: (urn) => void;
    onEdited?: (newQuery) => void;
    showDetails?: boolean;
};

const QueryCard = styled.div`
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 4px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    height: 380px;
    width: 32%;
    margin-right: 6px;
    margin-left: 6px;
    margin-bottom: 20px;
    overflow: hidden;
`;

const QueryCardHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: right;
`;

const QueryTitle = styled(Typography.Title)`
    && {
        margin: 0px;
        padding: 0px;
        color: ${ANTD_GRAY[9]};
    }
`;

const QueryDetails = styled.div`
    padding: 0px 20px 0px 20px;
`;

const QueryHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    white-space: nowrap;
`;

const QueryActions = styled.div<{ opacity?: number }>`
    padding: 0px;
    height: 0px;
    transform: translate(-4px, 12px);
    opacity: ${(props) => props.opacity || 0.3};
`;

const QueryAction = styled.span`
    margin-right: 8px;
`;

const QueryDetailsActions = styled.div``;

const EditQueryActionButton = styled(Button)`
    && {
        margin: 0px;
        padding: 0px 4px 0px 4px;
    }
`;

const EditQueryAction = styled.span`
    && {
        margin: 0px;
        padding: 0px;
        margin-left: 4px;
    }
`;

const QueryContainer = styled.div<{ fullHeight?: boolean }>`
    background-color: ${ANTD_GRAY[2]};
    margin: 0px 0px 4px 0px;
    height: ${(props) => (props.fullHeight && '380px') || '240px'};
    border-radius: 8px;
    :hover {
        cursor: pointer;
    }
`;

const QueryDescription = styled.div`
    margin-bottom: 16px;
`;

const MoreButton = styled.span`
    :hover {
        cursor: pointer;
    }
    font-weight: bold;
`;

const Date = styled.div`
    display: flex;
    justify-content: right;
    align-items: center;
`;

const EmptyContentText = styled.div`
    && {
        color: ${ANTD_GRAY[6]};
    }
`;

// NOTE: Yes, using `!important` is a shame. However, the SyntaxHighlighter is applying styles directly
// to the component, so there's no way around this
const NestedSyntax = styled(SyntaxHighlighter)`
    background-color: transparent !important;
    border: none !important;
    height: 100%;
    margin: 0px;
`;

export default function Query({
    urn,
    query,
    title,
    description,
    createdAtMs,
    createdBy,
    onDeleted,
    onEdited,
    showDelete,
    showEdit,
    showDetails = true,
}: Props) {
    console.log(createdBy);

    const [showQueryModal, setShowQueryModal] = useState(false);
    const [showEditQueryModal, setShowEditQueryModal] = useState(false);
    const [focused, setFocused] = useState(false);

    const [deleteQueryMutation] = useDeleteQueryMutation();

    const deleteQuery = () => {
        if (urn) {
            deleteQueryMutation({ variables: { urn } })
                .then(({ errors }) => {
                    if (!errors) {
                        message.success({
                            content: `Deleted Query!`,
                            duration: 3,
                        });
                        onDeleted?.(urn);
                    }
                })
                .catch(() => {
                    message.destroy();
                    message.error({ content: 'Failed to delete Query! An unexpected error occurred' });
                });
        }
    };

    const confirmDeleteQuery = () => {
        Modal.confirm({
            title: `Delete Query`,
            content: `Are you sure you want to delete this query?`,
            onOk() {
                deleteQuery();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const onEditQuery = () => {
        setShowEditQueryModal(true);
    };

    const onEditSubmitted = (newQuery) => {
        setShowEditQueryModal(false);
        onEdited?.(newQuery);
    };

    if (!query) {
        return null;
    }

    return (
        <>
            <QueryCard onMouseEnter={() => setFocused(true)} onMouseLeave={() => setFocused(false)}>
                <QueryCardHeader>
                    <QueryActions opacity={focused ? 1.0 : 0.3}>
                        <QueryAction>
                            <CopyQuery query={query} />
                        </QueryAction>
                        <QueryAction>
                            <Button onClick={() => setShowQueryModal(true)}>
                                <ExpandOutlined />
                            </Button>
                        </QueryAction>
                    </QueryActions>
                </QueryCardHeader>
                <QueryContainer fullHeight={!showDetails} onClick={() => setShowQueryModal(true)}>
                    <pre>
                        <NestedSyntax showLineNumbers language="sql">
                            {query}
                        </NestedSyntax>
                    </pre>
                </QueryContainer>
                {showDetails && (
                    <QueryDetails>
                        <QueryHeader>
                            <QueryTitle
                                style={{
                                    maxHeight: 40,
                                    overflow: 'hidden',
                                    textOverflow: 'ellipsis',
                                    whiteSpace: 'nowrap',
                                }}
                                level={5}
                            >
                                {title || <EmptyContentText>No title</EmptyContentText>}
                            </QueryTitle>
                            <QueryDetailsActions>
                                <span style={{ flexWrap: 'nowrap' }}>
                                    {showEdit && (
                                        <EditQueryAction>
                                            <EditQueryActionButton type="text" onClick={onEditQuery}>
                                                <EditOutlined />
                                            </EditQueryActionButton>
                                        </EditQueryAction>
                                    )}
                                    {showDelete && (
                                        <EditQueryAction>
                                            <Dropdown
                                                overlay={
                                                    <Menu>
                                                        <Menu.Item key="0" onClick={confirmDeleteQuery}>
                                                            <DeleteOutlined /> &nbsp; Delete
                                                        </Menu.Item>
                                                    </Menu>
                                                }
                                                trigger={['click']}
                                            >
                                                <MoreOutlined
                                                    data-testid="query-edit-button"
                                                    style={{ fontSize: 14 }}
                                                />
                                            </Dropdown>
                                        </EditQueryAction>
                                    )}
                                </span>
                            </QueryDetailsActions>
                        </QueryHeader>
                        <QueryDescription style={{ height: 52, overflow: 'scroll' }}>
                            {(description && (
                                <NoMarkdownViewer
                                    shouldWrap
                                    limit={200}
                                    readMore={<MoreButton onClick={() => setShowQueryModal(true)}>more</MoreButton>}
                                >
                                    {description}
                                </NoMarkdownViewer>
                            )) || <EmptyContentText>No description</EmptyContentText>}
                        </QueryDescription>
                        <Date>
                            {createdAtMs && (
                                <Typography.Text type="secondary">
                                    Created on {toLocalDateString(createdAtMs)}
                                </Typography.Text>
                            )}
                        </Date>
                    </QueryDetails>
                )}
            </QueryCard>
            {showQueryModal && (
                <QueryModal
                    query={query}
                    title={title}
                    description={description}
                    onClose={() => setShowQueryModal(false)}
                    showDetails={showDetails}
                />
            )}
            {showEditQueryModal && (
                <QueryBuilderModal
                    initialState={{ urn: urn as string, title, description, query }}
                    onSubmit={onEditSubmitted}
                    onClose={() => setShowEditQueryModal(false)}
                />
            )}
        </>
    );
}
