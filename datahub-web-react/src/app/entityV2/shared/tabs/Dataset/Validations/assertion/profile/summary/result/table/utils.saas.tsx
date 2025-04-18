import React from 'react';
import { Assertion, AssertionResultType, AssertionRunEvent, Monitor } from '@src/types.generated';
import { useAppConfig } from '@src/app/useAppConfig';
import { AssertionResultDot } from '../../../shared/AssertionResultDot';
import { getResultColor } from '../../../../../assertionUtils';
import { AssertionPredictionTableItem } from './AssertionPredictionTableItem.saas';

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
