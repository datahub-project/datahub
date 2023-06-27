declare global {
    interface Window {
        ATL_JQ_PAGE_PROPS: any;
        ADDITIONAL_CUSTOM_CONTEXT?: any;
    }
}

export const setupJiraIssueCollector = (customButtonId: string, entityUrn: string) => {
    const getEntityUrn = () => entityUrn;

    if (!window.ATL_JQ_PAGE_PROPS) {
        window.ATL_JQ_PAGE_PROPS = {
            triggerFunction(showCollectorDialog: () => void) {
                const button = document.getElementById(customButtonId);
                button?.addEventListener('click', (e) => {
                    e.preventDefault();
                    showCollectorDialog();
                });
            },
            environment() {
                const envInfo = {};

                if (window.ADDITIONAL_CUSTOM_CONTEXT) {
                    envInfo['Additional Context Information'] = window.ADDITIONAL_CUSTOM_CONTEXT;
                }

                return envInfo;
            },
            fieldValues() {
                const values: { summary?: string; fullname?: string; email?: string } = {};

                values.summary = getEntityUrn();
                values.fullname = 'Enter Sharp Username';
                values.email = 'email@sharp.com';

                return values;
            },
        };
    } else {
        window.ATL_JQ_PAGE_PROPS.fieldValues = function () {
            const values: { summary?: string; fullname?: string; email?: string } = {};

            values.summary = getEntityUrn();
            values.fullname = 'Enter Sharp Username';
            values.email = 'email@sharp.com';

            return values;
        };
    }
};
