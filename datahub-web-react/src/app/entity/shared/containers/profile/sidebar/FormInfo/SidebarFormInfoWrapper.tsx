import React, { useState } from 'react';
import styled from 'styled-components';
import EntityFormModal from '../../../../entityForm/EntityFormModal';
import FormInfo from './FormInfo';
import { StyledDivider } from './components';
import { useEntityData } from '../../../../EntityContext';
import { filterFormAssociationsForUser, getFormAssociations } from './utils';
import FormSelectionModal from '../../../../entityForm/FormSelectionModal/FormSelectionModal';
import analytics, { DocRequestCTASource, EventType } from '../../../../../../analytics';

const FormInfoWrapper = styled.div`
    margin-top: 16px;
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
                <StyledDivider />
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
