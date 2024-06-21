import React, { useState } from 'react';

import { Button } from 'antd';
import styled from 'styled-components';

import { useEntityData } from '../../EntityContext';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { useEntityFormContext } from '../EntityFormContext';

import { PromptSubTitle } from '../prompts/StructuredPropertyPrompt/StructuredPropertyPrompt';
import { BulkNavigationWrapper } from '../FormHeader/components';
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

export default function BulkVerifyHeader() {
    const {
        form: { form },
        entity: { selectedEntities },
    } = useEntityFormContext();
    const { entityType } = useEntityData();

    const [isVerifyModalVisible, setIsVerifyModalVisible] = useState(false);
    const entityRegistry = useEntityRegistry();

    const title = form?.info.name;
    const description = form?.info.description;

    return (
        <BulkVerifyWrapper $hideBackground>
            <div>
                <IntroTitle>
                    {title ? (
                        <>{title}</>
                    ) : (
                        <>{entityType ? entityRegistry.getEntityName(entityType) : 'Asset'} Requirements</>
                    )}
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
