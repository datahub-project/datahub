import { EmptyState, Pagination, SearchBar, toast } from '@components';
import { Stack } from '@phosphor-icons/react/dist/csr/Stack';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { EnvironmentBuilderModal } from '@app/settingsV2/environments/EnvironmentBuilderModal';
import { EnvironmentTable } from '@app/settingsV2/environments/EnvironmentTable';
import { filterEnvironments } from '@app/settingsV2/environments/environments.utils';
import { Message } from '@app/sharedV2/Message';
import { scrollToTop } from '@app/sharedV2/searchUtils';

import { useListCustomFabricTypesQuery } from '@graphql/customFabricType.generated';
import { CustomFabricType } from '@types';

// Environments are few; the server lists them all in one call and the list is filtered here.
const MAX_ENVIRONMENTS = 1000;
const PAGE_SIZE = 10;

type Props = {
    showBuilder: boolean;
    setShowBuilder: (show: boolean) => void;
    canManage: boolean;
};

export const EnvironmentList = ({ showBuilder, setShowBuilder, canManage }: Props) => {
    const { t } = useTranslation('settings.environments');
    const [page, setPage] = useState(1);
    const [query, setQuery] = useState('');
    const [editing, setEditing] = useState<CustomFabricType | undefined>(undefined);

    const { data, loading, error, refetch } = useListCustomFabricTypesQuery({
        variables: { input: { start: 0, count: MAX_ENVIRONMENTS, includeDeprecated: true } },
        fetchPolicy: 'cache-and-network',
    });

    useEffect(() => {
        if (error) {
            toast.error(t('loadError'));
        }
    }, [error, t]);

    const environments = useMemo(
        () => filterEnvironments((data?.listCustomFabricTypes?.customFabricTypes || []) as CustomFabricType[], query),
        [data, query],
    );
    const pageItems = environments.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

    const onEdit = (environment: CustomFabricType) => {
        setEditing(environment);
        setShowBuilder(true);
    };

    const onCloseBuilder = () => {
        setShowBuilder(false);
        setEditing(undefined);
    };

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    return (
        <>
            {!data && loading && <Message type="loading" content={t('loading')} />}
            <SearchBar
                placeholder={t('searchPlaceholder')}
                value={query}
                onChange={(value) => {
                    setQuery(value);
                    setPage(1);
                }}
                width="300px"
                allowClear
            />
            {environments.length > 0 ? (
                <>
                    <EnvironmentTable
                        environments={pageItems}
                        canManage={canManage}
                        onEdit={onEdit}
                        refetch={refetch}
                    />
                    {environments.length > PAGE_SIZE && (
                        <Pagination
                            currentPage={page}
                            itemsPerPage={PAGE_SIZE}
                            total={environments.length}
                            showLessItems
                            onPageChange={onChangePage}
                            showSizeChanger={false}
                        />
                    )}
                </>
            ) : (
                !loading && (
                    <EmptyState
                        title={t('emptyTitle')}
                        description={t('emptyDescription')}
                        icon={Stack}
                        action={
                            canManage
                                ? { label: t('createEnvironment'), onClick: () => setShowBuilder(true) }
                                : undefined
                        }
                        style={{ flex: 1, justifyContent: 'center' }}
                    />
                )
            )}
            {canManage && (
                <EnvironmentBuilderModal
                    isOpen={showBuilder}
                    onClose={onCloseBuilder}
                    refetch={refetch}
                    environment={editing}
                />
            )}
        </>
    );
};
