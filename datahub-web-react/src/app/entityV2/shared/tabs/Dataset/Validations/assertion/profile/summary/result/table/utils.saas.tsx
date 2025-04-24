import React from 'react';

import { AssertionResultDot } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/AssertionResultDot';
import { AssertionPredictionTableItem } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/table/AssertionPredictionTableItem.saas';
import { getResultColor } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';
import { useAppConfig } from '@src/app/useAppConfig';
import { Assertion, AssertionResultType, AssertionRunEvent, Monitor } from '@src/types.generated';

export const useAssertionPredictionItem = (assertion: Assertion, monitor?: Monitor) => {
    // We can only show predictions if smart assertions are being trained online
    const { onlineSmartAssertionsEnabled } = useAppConfig().config.featureFlags;
    if (!onlineSmartAssertionsEnabled) {
        return null;
    }

    const predictedAssertions = monitor?.info?.assertionMonitor?.assertions?.find(
        (assrn) => assrn.assertion.urn === assertion.urn,
    )?.context?.embeddedAssertions;
    return predictedAssertions?.length
        ? {
              dot: (
                  <div style={{ paddingTop: 12 }}>
                      <AssertionResultDot run={{ result: { type: AssertionResultType.Init } } as AssertionRunEvent} />
                  </div>
              ),
              color: getResultColor(AssertionResultType.Init),
              children: <AssertionPredictionTableItem predictions={predictedAssertions} />,
              key: 'assertion-prediction',
          }
        : null;
};
