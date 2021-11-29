import React, { useState } from 'react';
import { Button, Empty, List, message, Pagination } from 'antd';
import styled from 'styled-components';
import { PlusOutlined } from '@ant-design/icons';
import {
    useCreateIngestionExecutionRequestMutation,
    useListIngestionSourcesQuery,
} from '../../graphql/ingestion.generated';
import { Message } from '../shared/Message';
import TabToolbar from '../entity/shared/components/styled/TabToolbar';
import { IngestionSource } from '../../types.generated';
import IngestionSourceListItem from './IngestionSourceListItem';
import { IngestionSourceBuilderModal } from './IngestionSourceBuilderModal';
import { IngestionSourceDetailsModal } from './IngestionSourceDetailsModal';

const SourceContainer = styled.div``;

const SourceStyledList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
    }
`;

const SourcePaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const DEFAULT_PAGE_SIZE = 25;

export const IngestionSourceList = () => {
    const [page, setPage] = useState(1);

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    // Whether or not there is an urn to show in the modal
    const [isEditingSource, setIsEditingSource] = useState<boolean>(false);
    const [focusSourceUrn, setFocusSourceUrn] = useState<undefined | string>(undefined);

    // Set of removed urns used to account for eventual consistency
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);

    const { loading, error, data, refetch } = useListIngestionSourcesQuery({
        variables: {
            input: {
                start,
                count: pageSize,
            },
        },
        fetchPolicy: 'no-cache',
    });
    const [createExecutionRequestMutation] = useCreateIngestionExecutionRequestMutation();

    const totalSources = data?.listIngestionSources?.total || 0;
    const sources = data?.listIngestionSources?.ingestionSources || [];
    const filteredSources = sources.filter((user) => !removedUrns.includes(user.urn));

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    const handleDelete = (urn: string) => {
        // Hack to deal with eventual consistency.
        const newRemovedUrns = [...removedUrns, urn];
        setRemovedUrns(newRemovedUrns);
        setTimeout(function () {
            refetch?.();
        }, 3000);
    };

    const handleClick = (urn: string) => {
        setFocusSourceUrn(urn);
    };

    const handleExecute = (urn: string) => {
        createExecutionRequestMutation({
            variables: {
                input: {
                    ingestionSourceUrn: urn,
                },
            },
        })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to execute ingestion source!: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                message.success({
                    content: `Successfully submitted ingestion job!`,
                    duration: 3,
                });
                setIsEditingSource(false);
            });
    };

    const handleSubmit = (ingestionSource: any) => {
        console.log(ingestionSource);
        setIsEditingSource(false);
        setFocusSourceUrn(undefined);
    };

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading ingestion sources..." />}
            {error && message.error('Failed to load ingestion sources :(')}
            <SourceContainer>
                <TabToolbar>
                    <div>
                        <Button type="text" onClick={() => setIsEditingSource(true)}>
                            <PlusOutlined /> Create new source
                        </Button>
                    </div>
                </TabToolbar>
                <SourceStyledList
                    bordered
                    locale={{
                        emptyText: <Empty description="No Ingestion Sources!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                    }}
                    dataSource={filteredSources}
                    renderItem={(item: any) => (
                        <IngestionSourceListItem
                            source={item as IngestionSource}
                            onClick={() => handleClick(item.urn)}
                            onExecute={() => handleExecute(item.urn)}
                            onDelete={() => handleDelete(item.urn)}
                        />
                    )}
                />
                <SourcePaginationContainer>
                    <Pagination
                        style={{ margin: 40 }}
                        current={page}
                        pageSize={pageSize}
                        total={totalSources}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                </SourcePaginationContainer>
            </SourceContainer>
            <IngestionSourceBuilderModal
                visible={isEditingSource}
                onSubmit={handleSubmit}
                onCancel={() => setIsEditingSource(false)}
            />
            {focusSourceUrn && (
                <IngestionSourceDetailsModal
                    urn={focusSourceUrn}
                    visible={focusSourceUrn !== undefined}
                    onClose={() => setFocusSourceUrn(undefined)}
                />
            )}
        </>
    );
};
