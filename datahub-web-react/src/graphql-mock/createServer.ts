/* eslint-disable global-require */
/* eslint-disable @typescript-eslint/no-var-requires */

if (process.env.REACT_APP_MOCK === 'true' || process.env.REACT_APP_MOCK === 'cy') {
    if (process.env.REACT_APP_MOCK === 'cy') {
        require('./server').makeServerForCypress();
    } else {
        require('./server').makeServer();
    }
}

export {};
