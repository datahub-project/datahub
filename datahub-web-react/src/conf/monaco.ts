// eslint-disable-next-line @typescript-eslint/no-restricted-imports
import { loader } from '@monaco-editor/react';

import { resolveRuntimePath } from '@utils/runtimeBasePath';

loader.config({
    paths: {
        vs: resolveRuntimePath('/node_modules/monaco-editor/min/vs'),
    },
});
