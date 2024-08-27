/**
 * The object represents the state of the Ingestion Source Builder form.
 */
export interface SecretBuilderState {
    /**
     * The name of the secret.
     */
    urn?: string;
    /**
     * The name of the secret.
     */
    name?: string;

    /**
     * The value of the secret.
     */
    value?: string;

    /**
     * An optional description for the secret.
     */
    description?: string;
}
