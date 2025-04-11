import React, { useState } from 'react';
import styled from 'styled-components';
import EntityFormModal from '../../../../entityForm/EntityFormModal';
import FormInfo from './FormInfo';
import analytics, { DocRequestCTASource, EventType } from '../../../../../../analytics';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { getFormAssociations } from '../../../../../../entity/shared/containers/profile/sidebar/FormInfo/utils';
import FormSelectionModal from '../../../../../../entity/shared/entityForm/FormSelectionModal/FormSelectionModal';

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
