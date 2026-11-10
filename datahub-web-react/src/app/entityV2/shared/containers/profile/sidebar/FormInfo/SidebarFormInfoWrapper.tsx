import React, { useState } from 'react';
import styled from 'styled-components';

import analytics, { DocRequestCTASource, EventType } from '@app/analytics';
import { useEntityData } from '@app/entityV2/shared/EntityContext';
import FormInfo from '@app/entityV2/shared/containers/profile/sidebar/FormInfo/FormInfo';
import { getFormAssociations } from '@app/entityV2/shared/containers/profile/sidebar/FormInfo/utils';
import EntityFormModal from '@app/entityV2/shared/entityForm/EntityFormModal';
import FormSelectionModal from '@app/entityV2/shared/entityForm/FormSelectionModal/FormSelectionModal';

const FormInfoWrapper = styled.div`
    padding: 12px 0px;
    border-bottom: 1px dashed;
    border-color: ${(props) => props.theme.colors.border};
`;

export default function SidebarFormInfoWrapper() {
    const { entityData } = useEntityData();
    const [isFormSelectionModalVisible, setIsFormSelectionModalVisible] = useState(false);
    const [isFormVisible, setIsFormVisible] = useState(false);
    const [selectedFormUrn, setSelectedFormUrn] = useState<string | null>(null);
    const formAssociations = getFormAssociations(entityData);

    if (!formAssociations.length) return null;

    function openFormModal() {
        analytics.event({
            type: EventType.ClickDocRequestCTA,
            source: DocRequestCTASource.AssetPage,
        });

        if (formAssociations.length === 1) {
            setSelectedFormUrn(formAssociations[0].form.urn);
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
