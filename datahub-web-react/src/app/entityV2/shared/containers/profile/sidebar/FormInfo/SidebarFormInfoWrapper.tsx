import React, { useState } from 'react';
import styled from 'styled-components';

import analytics, { DocRequestCTASource, EventType } from '@app/analytics';
import { useEntityData } from '@app/entity/shared/EntityContext';
import {
    filterFormAssociationsForUser,
    getFormAssociations,
} from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';
import FormSelectionModal from '@app/entity/shared/entityForm/FormSelectionModal/FormSelectionModal';
import FormInfo from '@app/entityV2/shared/containers/profile/sidebar/FormInfo/FormInfo';
import EntityFormModal from '@app/entityV2/shared/entityForm/EntityFormModal';

const FormInfoWrapper = styled.div`
    padding: 12px 0px;
    border-bottom: 1px dashed;
    border-color: rgba(0, 0, 0, 0.3);
`;

export default function SidebarFormInfoWrapper() {
    const { entityData } = useEntityData();
    const [isFormSelectionModalVisible, setIsFormSelectionModalVisible] = useState(false);
    const [isFormVisible, setIsFormVisible] = useState(false);
    const [selectedFormUrn, setSelectedFormUrn] = useState<string | null>(null);
    const formAssociations = getFormAssociations(entityData);
    const filteredAssociations = formAssociations.filter((association) => filterFormAssociationsForUser(association));

    if (!filteredAssociations.length) return null;

    function openFormModal() {
        analytics.event({
            type: EventType.ClickDocRequestCTA,
            source: DocRequestCTASource.AssetPage,
        });

        if (filteredAssociations.length === 1) {
            setSelectedFormUrn(filteredAssociations[0].form.urn);
            setIsFormVisible(true);
        } else {
            setIsFormSelectionModalVisible(true);
        }
    }

    function selectFormUrn(urn: string) {
        setSelectedFormUrn(urn);
        setIsFormVisible(true);
        setIsFormSelectionModalVisible(false);
    }

    return (
        <>
            <FormInfoWrapper>
                <FormInfo openFormModal={openFormModal} />
            </FormInfoWrapper>
            <EntityFormModal
                selectedFormUrn={selectedFormUrn}
                isFormVisible={isFormVisible}
                hideFormModal={() => setIsFormVisible(false)}
            />
            <FormSelectionModal
                isFormSelectionModalVisible={isFormSelectionModalVisible}
                hideFormSelectionModal={() => setIsFormSelectionModalVisible(false)}
                selectFormUrn={selectFormUrn}
            />
        </>
    );
}
