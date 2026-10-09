import { Modal, SimpleSelect, Text, toast } from '@components';
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

type ApplicationOption = {
    value: string;
    label: string;
};

export const SetApplicationModal = ({ urns, onCloseModal, refetch }: Props) => {
    const { t } = useTranslation('entity.shared.containers');
    const { t: tc } = useTranslation('common.actions');
    const { t: tm } = useTranslation('misc');
    const [applicationUrn, setApplicationUrn] = useState<string | undefined>(undefined);
    const [selectedOption, setSelectedOption] = useState<ApplicationOption | undefined>(undefined);

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
                toast.success(t('sidebar.application.setSuccess'), { duration: 2 });
                refetch?.();
            })
            .catch((e: unknown) => {
                toast.destroy();
                if (e instanceof Error) {
                    toast.error(t('sidebar.application.setFailed', { message: e.message || '' }), { duration: 3 });
                }
            })
            .finally(() => {
                onCloseModal();
            });
    };

    const applicationOptions: ApplicationOption[] = useMemo(
        () =>
            data?.searchAcrossEntities?.searchResults
                ?.map((r) => r.entity)
                .filter((entity): entity is Application => entity.__typename === 'Application')
                .map((appEntity) => ({
                    value: appEntity.urn,
                    label: appEntity.properties?.name || '',
                })) || [],
        [data],
    );

    // Keep the chosen application in the option list after search results change,
    // so SimpleSelect can still resolve its closed-state label.
    const combinedOptions: ApplicationOption[] = useMemo(() => {
        if (!selectedOption) {
            return applicationOptions;
        }
        if (applicationOptions.some((option) => option.value === selectedOption.value)) {
            return applicationOptions;
        }
        return [selectedOption, ...applicationOptions];
    }, [applicationOptions, selectedOption]);

    const handleUpdate = (values: string[]) => {
        const urn = values[0];
        setApplicationUrn(urn);
        if (!urn) {
            setSelectedOption(undefined);
            return;
        }
        const match =
            applicationOptions.find((option) => option.value === urn) ??
            (selectedOption?.value === urn ? selectedOption : undefined);
        setSelectedOption(match);
    };

    return (
        <Modal
            title={t('sidebar.application.modalTitle')}
            open
            onCancel={onCloseModal}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onCloseModal,
                },
                {
                    text: tc('add'),
                    variant: 'filled',
                    disabled: !applicationUrn,
                    onClick: onOk,
                },
            ]}
        >
            <SimpleSelect
                dataTestId="application-select"
                showSearch
                width="full"
                placeholder={t('sidebar.application.selectPlaceholder')}
                values={applicationUrn ? [applicationUrn] : []}
                onUpdate={handleUpdate}
                onSearchChange={handleSearch}
                filterResultsByQuery={false}
                options={applicationOptions}
                combinedSelectedAndSearchOptions={combinedOptions}
                isLoading={loading}
                optionDataTestId={(option) => `application-option-${option.value}`}
                emptyState={loading ? undefined : <Text size="sm">{tm('applications.empty')}</Text>}
            />
            {error && (
                <Text size="sm" color="red">
                    {t('sidebar.application.loadFailed', { message: error.message })}
                </Text>
            )}
        </Modal>
    );
};
