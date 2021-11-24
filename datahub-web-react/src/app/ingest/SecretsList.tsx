import React, { useState } from 'react';
import { Button, Empty, List, message, Pagination } from 'antd';
import { PlusOutlined } from '@ant-design/icons';

import styled from 'styled-components';
import { useListSecretsQuery } from '../../graphql/ingestion.generated';
import { Message } from '../shared/Message';
import TabToolbar from '../entity/shared/components/styled/TabToolbar';
import { CreateSecretInput, Secret } from '../../types.generated';
import SecretListItem from './SecretListItem';
import { SecretDetailsModal } from './SecretDetailsModal';
import { SecretBuilderModal } from './SecretBuilderModal';

const SourceContainer = styled.div``;

const SourceStyledList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
    }a
`;

const SourcePaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const DEFAULT_PAGE_SIZE = 25;

export const SecretsList = () => {
    const [page, setPage] = useState(1);

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    // Whether or not there is an urn to show in the modal
    const [isCreatingSecret, setIsCreatingSecret] = useState<boolean>(false);
    const [focusSecretUrn, setFocusSecretUrn] = useState<undefined | string>(undefined);

    // Set of removed urns used to account for eventual consistency
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);

    const { loading, error, data, refetch } = useListSecretsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
            },
        },
        fetchPolicy: 'no-cache',
    });

    const totalSecrets = data?.listSecrets?.total || 0;
    const secrets = data?.listSecrets?.secrets || [];
    const filteredSecrets = secrets.filter((user) => !removedUrns.includes(user.urn));
    const focusSecret = secrets.find((secret) => secret.urn === focusSecretUrn);

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
        setFocusSecretUrn(urn);
    };

    const handleSubmit = (_: CreateSecretInput) => {
        setIsCreatingSecret(false);
    };

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading secrets..." />}
            {error && message.error('Failed to load secrets :(')}
            <SourceContainer>
                <TabToolbar>
                    <div>
                        <Button type="text" onClick={() => setIsCreatingSecret(true)}>
                            <PlusOutlined /> Create new secret
                        </Button>
                    </div>
                </TabToolbar>
                <SourceStyledList
                    bordered
                    locale={{
                        emptyText: <Empty description="No Secrets!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                    }}
                    dataSource={filteredSecrets}
                    renderItem={(item: any) => (
                        <SecretListItem
                            secret={item as Secret}
                            onClick={() => handleClick(item.urn)}
                            onDelete={() => handleDelete(item.urn)}
                        />
                    )}
                />
                <SourcePaginationContainer>
                    <Pagination
                        style={{ margin: 40 }}
                        current={page}
                        pageSize={pageSize}
                        total={totalSecrets}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                </SourcePaginationContainer>
            </SourceContainer>
            <SecretBuilderModal
                visible={isCreatingSecret}
                onSubmit={handleSubmit}
                onCancel={() => setIsCreatingSecret(false)}
            />
            {focusSecret && (
                <SecretDetailsModal
                    secret={focusSecret}
                    visible={focusSecretUrn !== undefined}
                    onClose={() => setFocusSecretUrn(undefined)}
                />
            )}
        </>
    );
};
