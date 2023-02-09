import React, { useState } from 'react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { EditOutlined, ExpandOutlined, MoreOutlined } from '@ant-design/icons';
import { Button, message, Typography } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';
import { CorpUser } from '../../../../../../types.generated';
import QueryModal from './QueryModal';
import CopyQuery from './CopyQuery';
import { useDeleteQueryMutation } from '../../../../../../graphql/query.generated';
import { toLocalDateString } from '../../../../../shared/time/timeUtils';
import QueryBuilderModal from './QueryBuilderModal';

export type Props = {
    urn?: string;
    query?: string | null;
    title?: string;
    description?: string;
    createdAtMs?: number;
    executedAtMs?: number;
    createdBy?: CorpUser;
    showDelete?: boolean;
    showEdit?: boolean;
    filterText?: string;
    onDeleted?: () => void;
    onEdited?: () => void;
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
        padding-left: 20px;
        color: ${ANTD_GRAY[9]};
    }
`;

const QueryHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
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

const EditQueryActionButton = styled(Button)`
    && {
        margin: 0px;
        padding: 0px;
        margin-right: 8px;
        padding 2px;
    }
`;

const EditQueryAction = styled.span`
    && {
        margin: 0px;
        padding: 0px;
    }
`;

const QueryContainer = styled.div`
    overflow-y: scroll;
    background-color: ${ANTD_GRAY[2]};
    margin: 0px 0px 4px 0px;
    height: 240px;
    border-radius: 8px;
    :hover {
        cursor: pointer;
    }
`;

// NOTE: Yes, using `!important` is a shame. However, the SyntaxHighlighter is applying styles directly
// to the component, so there's no way around this
const NestedSyntax = styled(SyntaxHighlighter)`
    background-color: transparent !important;
    border: none !important;
`;

const DescriptionText = styled(Typography.Text)`
    padding-left: 20px;
`;

export default function Query({
    urn,
    query,
    title,
    description,
    createdAtMs,
    executedAtMs,
    createdBy,
    onDeleted,
    onEdited,
    showDelete,
    showEdit,
}: Props) {
    console.log(createdBy);

    const [showQueryModal, setShowQueryModal] = useState(false);
    const [showEditQueryModal, setShowEditQueryModal] = useState(false);
    const [focused, setFocused] = useState(false);

    const [deleteQueryMutation] = useDeleteQueryMutation();

    const onDeleteQuery = () => {
        if (urn) {
            deleteQueryMutation({ variables: { urn } })
                .then(({ errors }) => {
                    if (!errors) {
                        message.success({
                            content: `Deleted Query!`,
                            duration: 3,
                        });
                        onDeleted?.();
                    }
                })
                .catch(() => {
                    message.destroy();
                    message.error({ content: 'Failed to delete Query! An unexpected error occurred' });
                });
        }
    };

    const onEditQuery = () => {
        setShowEditQueryModal(true);
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
                <pre>
                    <QueryContainer onClick={() => setShowQueryModal(true)}>
                        <NestedSyntax showLineNumbers language="sql">
                            {query}
                        </NestedSyntax>
                    </QueryContainer>
                </pre>
                <QueryHeader>
                    {(title && <QueryTitle level={5}>{title || 'No title'}</QueryTitle>) || <div> </div>}
                    <div>
                        {showEdit && (
                            <EditQueryAction>
                                <EditQueryActionButton type="text" onClick={onEditQuery}>
                                    <EditOutlined />
                                </EditQueryActionButton>
                            </EditQueryAction>
                        )}
                        {showDelete && (
                            <EditQueryAction>
                                <EditQueryActionButton type="text" onClick={onDeleteQuery}>
                                    <MoreOutlined />
                                </EditQueryActionButton>
                            </EditQueryAction>
                        )}
                    </div>
                </QueryHeader>
                <DescriptionText>{description || 'No description'}</DescriptionText>
                <div style={{ marginLeft: 20 }}>
                    {executedAtMs && (
                        <Typography.Text type="secondary">
                            Executed on {toLocalDateString(executedAtMs)}
                        </Typography.Text>
                    )}
                </div>
                <div style={{ marginLeft: 20 }}>
                    {createdAtMs && (
                        <Typography.Text type="secondary">Created on {toLocalDateString(createdAtMs)}</Typography.Text>
                    )}
                </div>
            </QueryCard>
            {showQueryModal && (
                <QueryModal
                    query={query}
                    title={title}
                    description={description}
                    onClose={() => setShowQueryModal(false)}
                />
            )}
            {showEditQueryModal && (
                <QueryBuilderModal
                    initialState={{ title, description, query }}
                    onSubmit={onEdited}
                    onClose={() => setShowEditQueryModal(false)}
                />
            )}
        </>
    );
}
