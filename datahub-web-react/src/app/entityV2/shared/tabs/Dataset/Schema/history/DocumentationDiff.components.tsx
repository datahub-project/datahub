import React from 'react';
import ReactDiffViewer, { DiffMethod } from 'react-diff-viewer-continued';
import { useTheme } from 'styled-components';

type Props = {
    oldValue: string;
    newValue: string;
};

export default function DocumentationDiffViewer({ oldValue, newValue }: Props) {
    const theme = useTheme();

    const diffViewerStyles = {
        diffContainer: {
            fontSize: '12px',
        },
        wordAdded: {
            background: theme.colors.bgSurfaceSuccess,
            color: theme.colors.textSuccess,
        },
        wordRemoved: {
            background: theme.colors.bgSurfaceError,
            color: theme.colors.textError,
        },
    };

    // Descriptions are short, so diff synchronously instead of in the library's blob: Web Worker,
    // which a stricter DATAHUB_CSP_WORKER_SRC could block.
    return (
        <ReactDiffViewer
            oldValue={oldValue}
            newValue={newValue}
            compareMethod={DiffMethod.LINES}
            splitView={false}
            hideLineNumbers
            disableWorker
            styles={diffViewerStyles}
        />
    );
}
