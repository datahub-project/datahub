/* eslint-disable global-require */
/* eslint-disable @typescript-eslint/no-var-requires */

if (process.env.REACT_APP_MOCK === 'true') {
    require('./server').makeServer();
}

export {};
