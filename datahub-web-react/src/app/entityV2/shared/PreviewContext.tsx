import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { PreviewType } from '@app/entityV2/Entity';
import { AttributionDetails } from '@app/sharedV2/propagation/types';

type PreviewContextType = {
    previewData: GenericEntityProperties | null;
    previewType?: PreviewType;
    propagationDetails?: AttributionDetails;
};

export type PreviewContextProps = Omit<PreviewContextType, 'previewData' | 'previewType'>;

const PreviewContext = React.createContext<PreviewContextType>({
    previewData: null,
});
export default PreviewContext;

export function usePreviewData() {
    return React.useContext(PreviewContext);
}
