import React from 'react';
import ReactDOM from 'react-dom';
import { tryLoadAndStartRecorder } from '@alwaysmeticulous/recorder-loader';
import { App } from './App';
import reportWebVitals from './reportWebVitals';

async function startApp() {
    // Record all sessions on Dev01
    if (isDev01()) {
        // Start the Meticulous recorder before you initialise your app.
        // Note: all errors are caught and logged, so no need to surround with try/catch
        await tryLoadAndStartRecorder({
            recordingToken: import.meta.env.REACT_APP_METICULOUS_PROJECT_TOKEN,
            isProduction: false,
        });
    }

    // If you want to start measuring performance in your app, pass a function
    // to log results (for example: reportWebVitals(console.log))
    // or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
    reportWebVitals();

    // Initalise app after the Meticulous recorder is ready, e.g.
    ReactDOM.render(
        <React.StrictMode>
            <App />
        </React.StrictMode>,
        document.getElementById('root'),
    );
}

function isDev01() {
    return window.location.hostname.includes('dev01.acryl.io');
}

startApp();
