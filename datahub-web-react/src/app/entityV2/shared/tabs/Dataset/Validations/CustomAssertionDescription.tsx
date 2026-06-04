import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { DatasetAssertionLogicModal } from '@app/entityV2/shared/tabs/Dataset/Validations/DatasetAssertionLogicModal';

import { AssertionInfo } from '@types';

const ViewLogicButton = styled(Button)`
    padding: 0px;
    margin: 0px;
`;

type Props = {
    assertionInfo: AssertionInfo;
};

/**
 * A human-readable description of a Custom Assertion, surfacing its logic
 * (e.g. the SQL behind a Monte Carlo custom-SQL rule) behind a "View Logic" link.
 */
export const CustomAssertionDescription = ({ assertionInfo }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const [isLogicVisible, setIsLogicVisible] = useState(false);

    const { description } = assertionInfo;
    const logic = assertionInfo.customAssertion?.logic;

    return (
        <>
            <Typography.Text>{description}</Typography.Text>
            {logic && (
                <>
                    <div>
                        <ViewLogicButton onClick={() => setIsLogicVisible(true)} type="link">
                            {t('datasetDescription.popover.viewLogic')}
                        </ViewLogicButton>
                    </div>
                    <DatasetAssertionLogicModal
                        logic={logic}
                        title={description || undefined}
                        description={assertionInfo.customAssertion?.type || undefined}
                        visible={isLogicVisible}
                        onClose={() => setIsLogicVisible(false)}
                    />
                </>
            )}
        </>
    );
};
