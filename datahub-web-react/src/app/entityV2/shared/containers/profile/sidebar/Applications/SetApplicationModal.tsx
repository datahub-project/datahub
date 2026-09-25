import { Modal, Select, Typography, message } from 'antd';
import debounce from 'lodash/debounce';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { useBatchSetApplicationMutation, useGetApplicationsListLazyQuery } from '@graphql/application.generated';
import { Application, EntityType } from '@types';

const SEARCH_DEBOUNCE_MS = 300;
const DEFAULT_RESULT_COUNT = 100;
// Minimum chars before switching from wildcard to keyword search; short queries return no results
const MIN_SEARCH_LENGTH = 3;

interface Props {
    urns: string[];
    onCloseModal: () => void;
    refetch?: () => void;
}

export const SetApplicationModal = ({ urns, onCloseModal, refetch }: Props) => {
    const { t } = useTranslation('entity.shared.containers');
    const [applicationUrn, setApplicationUrn] = useState<string | undefined>(undefined);

    const [getApplications, { data, loading, error }] = useGetApplicationsListLazyQuery();

    useEffect(() => {
        getApplications({
            variables: {
                input: {
                    start: 0,
                    count: DEFAULT_RESULT_COUNT,
                    query: '*',
                    types: [EntityType.Application],
                },
            },
        });
    }, [getApplications]);

    const handleSearch = useMemo(() => {
        const fetch = (text: string) => {
            const trimmed = text.trim();
            getApplications({
                variables: {
                    input: {
                        start: 0,
                        count: DEFAULT_RESULT_COUNT,
                        query: trimmed.length >= MIN_SEARCH_LENGTH ? trimmed : '*',
                        types: [EntityType.Application],
                    },
                },
            });
        };
        return debounce(fetch, SEARCH_DEBOUNCE_MS);
    }, [getApplications]);

    const onSearch = (value: string) => {
        handleSearch(value);
    };

    const [batchSetApplicationMutation] = useBatchSetApplicationMutation();

    const onOk = () => {
        if (!applicationUrn) {
            return;
        }
        batchSetApplicationMutation({
            variables: {
                input: {
                    applicationUrn,
                    resourceUrns: urns,
                },
            },
        })
            .then(() => {
                message.success({ content: t('sidebar.application.setSuccess'), duration: 2 });
                refetch?.();
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: t('sidebar.application.setFailed', { message: e.message || '' }),
                        duration: 3,
                    });
                }
            })
            .finally(() => {
                onCloseModal();
            });
    };

    const applicationOptions =
        data?.searchAcrossEntities?.searchResults
            ?.map((r) => r.entity)
            .filter((entity): entity is Application => entity.__typename === 'Application')
            .map((appEntity) => {
                return {
                    value: appEntity.urn,
                    label: appEntity.properties?.name || '',
                    'data-testid': `application-option-${appEntity.urn}`,
                };
            }) || [];

    const notFoundContent = () => {
        if (loading) return null;
        return 'No applications found';
    };

    return (
        <Modal title={t('sidebar.application.modalTitle')} open onOk={onOk} onCancel={onCloseModal} closable>
            <Select
                data-testid="application-select"
                showSearch
                style={{ width: '100%' }}
                placeholder={t('sidebar.application.selectPlaceholder')}
                onChange={(value) => setApplicationUrn(value)}
                onSearch={onSearch}
                filterOption={false}
                options={applicationOptions}
                loading={loading}
                value={applicationUrn}
                notFoundContent={notFoundContent()}
            />
            {error && (
                <Typography.Text type="danger">
                    {t('sidebar.application.loadFailed', { message: error.message })}
                </Typography.Text>
            )}
        </Modal>
    );
};
