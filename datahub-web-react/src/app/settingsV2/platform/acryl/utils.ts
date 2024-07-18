export const ACRYL_PLATFORM_URN = 'urn:li:dataPlatform:acryl';

// format the connection json object before submitting to the backend
export const getConnectionBlob = (url: string, token: string): string => {
    const jsonObject = {
        connection: {
            server: url,
            token,
        },
    };
    return JSON.stringify(jsonObject);
};

export const getURLFromJson = (json) => {
    const parsedJson = JSON.parse(json || '{}');
    return parsedJson.connection?.server;
};

export const getTokenFromJson = (json) => {
    const parsedJson = JSON.parse(json || '{}');
    const token = parsedJson.connection?.token;
    return token;
};

export const showToken = (token, noOfCharacters) => {
    const tokenToDisplay = `**************${token.slice(-noOfCharacters)}`;
    return tokenToDisplay;
};
