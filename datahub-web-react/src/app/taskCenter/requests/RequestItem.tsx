import React, { useState } from 'react';

import { List } from 'antd';
import styled from 'styled-components';
import { Button, colors } from '@src/alchemy-components';
import EntityFormModal from '../../entity/shared/entityForm/EntityFormModal';
import { FormType } from '../../../types.generated';
import { pluralize } from '../../shared/textUtil';
import analytics, { DocRequestCTASource, EventType } from '../../analytics';
import { useAppConfig } from '../../useAppConfig';
import { FormView } from '../../entity/shared/entityForm/EntityFormContext';

const Title = styled.div`
    color: ${colors.gray[800]};
    font-weight: 600;
    font-size: 14px;
`;

const SubHeader = styled.div`
    color: ${colors.gray[1700]};
    font-size: 14px;
`;

type Props = {
    request: any;
    refetch: () => void;
};

export const RequestItem = ({ request, refetch }: Props) => {
    const appConfig = useAppConfig();
    const defaultFormView = appConfig.config.featureFlags.showBulkFormByDefault ? FormView.BY_QUESTION : undefined;
    const [modalOpen, setModalOpen] = useState(false);
    const { form, numEntitiesToComplete } = request;
    const { type, name } = form.info;

    // List of Owners
    const owners = form?.ownership?.owners;
    const isVerificationForm = type === FormType.Verification;

    // Messaging
    let message = isVerificationForm ? `New Verification Tasks` : `New Compliance Tasks`;
    if (owners && owners.length > 0) {
        const ownerName = owners[0].owner.info.displayName;
        if (ownerName)
            message = isVerificationForm
                ? `New Verification Tasks from ${ownerName}`
                : `New Compliance Tasks from ${ownerName}`;
    }

    // Close modal & refetch
    const closeModal = () => {
        refetch();
        setModalOpen(false);
    };

    const openModal = () => {
        setModalOpen(true);
        analytics.event({
            type: EventType.ClickDocRequestCTA,
            source: DocRequestCTASource.TaskCenter,
        });
    };

    const displayedNumEntities = numEntitiesToComplete >= 10000 ? '10,000+' : numEntitiesToComplete;

    return (
        <>
            <List.Item key={form.urn}>
                <div>
                    <Title>{message}</Title>
                    <SubHeader>
                        Please complete {name} for{' '}
                        <b>
                            {displayedNumEntities} {pluralize(numEntitiesToComplete, 'asset')}
                        </b>
                    </SubHeader>
                </div>
                <Button variant="text" onClick={openModal}>
                    Open in Compliance Center
                </Button>
            </List.Item>
            <EntityFormModal
                selectedFormUrn={form.urn}
                isFormVisible={modalOpen}
                hideFormModal={closeModal}
                defaultFormView={defaultFormView}
            />
        </>
    );
};
