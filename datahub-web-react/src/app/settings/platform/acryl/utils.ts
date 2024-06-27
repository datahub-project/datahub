export const ACRYL_PLATFORM_URN = 'urn:li:dataPlatform:acryl';

export const getConnectionBlob = (url: string, token: string): string => {
    const jsonObject = {
        connection: {
            server: url.endsWith('/gms') ? url : `${url}/gms`,
            token,
        },
    };
    return JSON.stringify(jsonObject);
};

export const getURLFromJson = (json) => {
    const parsedJson = JSON.parse(json);
    const server = parsedJson.connection?.server;
    const url = server?.endsWith('/gms') ? server.replace('/gms', '') : server;
    return url;
};

export const getTokenFromJson = (json) => {
    const parsedJson = JSON.parse(json);
    const token = parsedJson.connection?.token;
    return token;
};

export const showToken = (token, noOfCharacters) => {
    const tokenToDisplay = `**************${token.slice(-noOfCharacters)}`;
    return tokenToDisplay;
};
