import { Empty, List, message } from 'antd';
import React, { useMemo } from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import { Message } from '@app/shared/Message';
import { RequestItem } from '@app/taskCenterV2/requests/RequestItem';
import { filterFormsForUser } from '@app/taskCenterV2/requests/utils';
import { FormView } from '@src/app/entity/shared/entityForm/EntityFormContext';
import EntityFormModal from '@src/app/entity/shared/entityForm/EntityFormModal';
import { useAppConfig } from '@src/app/useAppConfig';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { FormForActor } from '@src/types.generated';

import { useGetFormsForActorQuery } from '@graphql/form.generated';

const StyledList = styled(List)<{ $isShowNavBarRedesign?: boolean }>`
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        overflow-x: hidden;
        overflow-y: auto;
        border-radius: 0 0 ${props.theme.styles['border-radius-navbar-redesign']} ${props.theme.styles['border-radius-navbar-redesign']};
    `}

    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
        flex: 1;
        overflow: auto;
        height: 100%;
    }

    .ant-spin-nested-loading,
    .ant-spin-container {
        height: 100%;
    }

    .ant-list-empty-text {
        display: flex;
        align-items: center;
        justify-content: center;
        height: 100%;
    }
`;

export const Requests = () => {
    const appConfig = useAppConfig();
    const history = useHistory();
    const location = useLocation();
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { data, loading, error, refetch } = useGetFormsForActorQuery({
        variables: { input: { searchFlags: { skipCache: true } } },
        fetchPolicy: 'no-cache',
    });
    const defaultFormView = appConfig.config.featureFlags.showBulkFormByDefault ? FormView.BY_QUESTION : undefined;

    // Get form_urn from URL query parameters
    const queryParams = new URLSearchParams(location.search);
    const encodedFormParam = queryParams.get('form_urn');
    const focusForm = encodedFormParam ? decodeURIComponent(encodedFormParam) : undefined;

    const requests = useMemo(
        () =>
            // client-side filter of forms unfinished
            (data?.getFormsForActor.formsForActor || []).filter((form) => filterFormsForUser(form as FormForActor)) ||
            [],
        [data],
    );

    // Function to select a form by updating the URL with the encoded form URN
    const selectForm = (formUrn: string) => {
        const encodedFormUrn = encodeURIComponent(formUrn);
        const url = new URL(window.location.href);
        url.searchParams.set('form_urn', encodedFormUrn);
        history.push(url.pathname + url.search);
    };

    // Close modal & refetch - now it removes the form_urn from the URL
    const closeModal = () => {
        refetch();
        const url = new URL(window.location.href);
        url.searchParams.delete('form_urn');
        history.push(url.pathname + url.search);
    };

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading your requests…" />}
            {error && message.error('Failed to load compliance requests. An unknown error occurred!')}
            <StyledList
                bordered
                locale={{
                    emptyText: <Empty description="No Requests!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                }}
                dataSource={requests}
                renderItem={(item: any) => (
                    <RequestItem request={item} onClickOpenRequest={() => selectForm(item.form.urn)} />
                )}
                $isShowNavBarRedesign={isShowNavBarRedesign}
            />
            {focusForm ? (
                <EntityFormModal
                    selectedFormUrn={focusForm}
                    isFormVisible
                    hideFormModal={closeModal}
                    defaultFormView={defaultFormView}
                />
            ) : null}
        </>
    );
};
