import { Button } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import BulkVerifyModal from '@app/entity/shared/entityForm/BulkVerify/BulkVerifyModal';
import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { BulkNavigationWrapper } from '@app/entity/shared/entityForm/FormHeader/components';
import { PromptSubTitle } from '@app/entity/shared/entityForm/prompts/StructuredPropertyPrompt/StructuredPropertyPrompt';
import { FORM_BULK_VERIFY_ID } from '@app/onboarding/config/FormOnboardingConfig';
import { pluralize } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

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

const ButtonsWrapper = styled.div`
    display: flex;
`;

export default function BulkVerifyHeader() {
    const {
        refetchForBulk,
        setShouldRefetch,
        form: { form },
        entity: { selectedEntities, areAllEntitiesSelected },
        search: { results },
    } = useEntityFormContext();
    const totalResults = results.searchAcrossEntities?.total || 0;
    const { entityType } = useEntityData();

    const [isVerifyModalVisible, setIsVerifyModalVisible] = useState(false);
    const entityRegistry = useEntityRegistry();

    function handleReload() {
        refetchForBulk();
        setShouldRefetch(true);
    }

    const title = form?.info?.name;
    const description = form?.info?.description;

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
            <ButtonsWrapper>
                <VerifyButton onClick={handleReload}>Reload</VerifyButton>
                <VerifyButton
                    type="primary"
                    disabled={!selectedEntities.length}
                    onClick={() => setIsVerifyModalVisible(true)}
                    id={FORM_BULK_VERIFY_ID}
                >
                    {areAllEntitiesSelected ? (
                        <>
                            Verify {totalResults} {pluralize(totalResults, 'Asset')}
                        </>
                    ) : (
                        <>
                            Verify {selectedEntities.length} {pluralize(selectedEntities.length, 'Asset')}
                        </>
                    )}
                </VerifyButton>
            </ButtonsWrapper>
            {isVerifyModalVisible && (
                <BulkVerifyModal
                    isVerifyModalVisible={isVerifyModalVisible}
                    closeModal={() => setIsVerifyModalVisible(false)}
                />
            )}
        </BulkVerifyWrapper>
    );
}
