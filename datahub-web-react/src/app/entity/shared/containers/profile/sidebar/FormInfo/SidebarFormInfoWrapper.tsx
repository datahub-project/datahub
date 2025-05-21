import React, { useState } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import FormInfo from '@app/entity/shared/containers/profile/sidebar/FormInfo/FormInfo';
import { StyledDivider } from '@app/entity/shared/containers/profile/sidebar/FormInfo/components';
import { getFormAssociations } from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';
import EntityFormModal from '@app/entity/shared/entityForm/EntityFormModal';
import FormSelectionModal from '@app/entity/shared/entityForm/FormSelectionModal/FormSelectionModal';

const FormInfoWrapper = styled.div`
    margin-top: 16px;
`;

export default function SidebarFormInfoWrapper() {
    const { entityData } = useEntityData();
    const [isFormSelectionModalVisible, setIsFormSelectionModalVisible] = useState(false);
    const [isFormVisible, setIsFormVisible] = useState(false);
    const [selectedFormUrn, setSelectedFormUrn] = useState<string | null>(null);
    const formAssociations = getFormAssociations(entityData);

    if (!formAssociations.length) return null;

    function openFormModal() {
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
