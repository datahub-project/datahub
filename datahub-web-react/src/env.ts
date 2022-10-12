declare global {
    interface Window {
        env: any;
    }
}
type EnvType = {
    CONTACTUS: string;
    FAQ: string;
    GUIDE: string;
};
export const env: EnvType = { ...process.env, ...window.env };
