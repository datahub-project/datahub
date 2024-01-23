import { Button } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { getFormAssociation } from '../../containers/profile/sidebar/FormInfo/utils';
import { useEntityData } from '../../EntityContext';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { PromptSubTitle } from '../prompts/StructuredPropertyPrompt/StructuredPropertyPrompt';
import { BulkNavigationWrapper } from './components';
import { useEntityFormContext } from '../EntityFormContext';
import { pluralize } from '../../../../shared/textUtil';
import BulkVerifyModal from './BulkVerifyModal';
import { FORM_BULK_VERIFY_ID } from '../../../../onboarding/config/FormOnboardingConfig';

const BulkVerifyWrapper = styled(BulkNavigationWrapper)`
    justify-content: space-between;
    align-items: center;
`;

const IntroTitle = styled.div`
    font-size: 20px;
    font-weight: 600;
    color: white;
`;

const SubTitle = styled(PromptSubTitle)`
    margin-top: 16px;
    color: white;
`;

const VerifyButton = styled(Button)`
    font-size: 14px;
    display: flex;
    align-items: center;
    margin-left: 8px;
`;

interface Props {
    formUrn: string;
}

export default function BulkVerifyHeader({ formUrn }: Props) {
    const [isVerifyModalVisible, setIsVerifyModalVisible] = useState(false);
    const { selectedEntities } = useEntityFormContext();
    const entityRegistry = useEntityRegistry();
    const { entityType, entityData } = useEntityData();
    const formAssociation = getFormAssociation(formUrn, entityData);
    const title = formAssociation?.form.info.name;
    const description = formAssociation?.form.info.description;

    return (
        <BulkVerifyWrapper $hideBackground>
            <div>
                <IntroTitle>
                    {title ? <>{title}</> : <>{entityRegistry.getEntityName(entityType)} Requirements</>}
                </IntroTitle>
                {description ? (
                    <SubTitle>{description}</SubTitle>
                ) : (
                    <SubTitle>
                        Please fill out the following information for this {entityRegistry.getEntityName(entityType)} so
                        that we can keep track of the status of the asset
                    </SubTitle>
                )}
            </div>

            <VerifyButton
                type="primary"
                disabled={!selectedEntities.length}
                onClick={() => setIsVerifyModalVisible(true)}
                id={FORM_BULK_VERIFY_ID}
            >
                Verify {selectedEntities.length} {pluralize(selectedEntities.length, 'Asset')}
            </VerifyButton>
            {isVerifyModalVisible && (
                <BulkVerifyModal
                    isVerifyModalVisible={isVerifyModalVisible}
                    closeModal={() => setIsVerifyModalVisible(false)}
                />
            )}
        </BulkVerifyWrapper>
    );
}
