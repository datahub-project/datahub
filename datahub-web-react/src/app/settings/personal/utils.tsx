export const validateSlackHandle = (value: string) => {
    if (value && !value.startsWith('#')) {
        return Promise.reject(new Error('Slack handle must start with #'));
    }
    return Promise.resolve();
};
