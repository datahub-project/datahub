import React from 'react';

import { ExternalDocumentInlineContentSection } from '@app/entityV2/document/preview/ExternalDocumentInlineContentSection';
import SummaryTab from '@app/entityV2/summary/SummaryTab';

export default function ExternalDocumentInlineSummaryTab() {
    return <SummaryTab properties={{ preContent: <ExternalDocumentInlineContentSection /> }} />;
}
