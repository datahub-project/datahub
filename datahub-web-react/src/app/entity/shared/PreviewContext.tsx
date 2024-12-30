import React from 'react';
import { GenericEntityProperties } from './types';

const PreviewContext = React.createContext<GenericEntityProperties | null>(null);
export default PreviewContext;

export function usePreviewData() {
    return React.useContext(PreviewContext);
}
