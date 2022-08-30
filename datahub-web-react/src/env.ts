declare global {
    interface Window {
        env: any;
    }
}
type EnvType = {
    CONTACTUS: string;
    FAQ: string;
};
export const env: EnvType = { ...process.env, ...window.env };
