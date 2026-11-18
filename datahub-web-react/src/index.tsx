// antd 5's message, notification and Modal.confirm use ReactDOM.render, removed in React 19.
// Drop this patch (also in setupTests and .storybook/preview) when migrating to antd 6.
import '@ant-design/v5-patch-for-react-19';
import React from 'react';
import { createRoot } from 'react-dom/client';

import { installChunkReloadHandler } from '@app/sharedV2/chunkReload';
import { App } from '@src/App';
import '@src/i18n/i18n';
import reportWebVitals from '@src/reportWebVitals';
import '@utils/dayjs';

installChunkReloadHandler();

createRoot(document.getElementById('root') as HTMLElement).render(
    <React.StrictMode>
        <App />
    </React.StrictMode>,
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
