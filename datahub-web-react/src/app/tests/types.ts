/**
 * The object represents the state of the Test Builder form.
 */
export interface TestBuilderState {
    /**
     * The name of the test.
     */
    name?: string;

    /**
     * The category of the test.
     */
    category?: string;

    /**
     * An optional description for the test.
     */
    description?: string | null;

    /**
     * The definition of the test
     */
    definition?: {
        /**
         * The JSON version of a test definition.
         */
        json?: string | null;
    };
}
