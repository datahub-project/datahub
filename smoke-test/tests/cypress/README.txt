# Running Cypress Tests Locally

1. Make sure the packages are installed. It uses some node modules to run locally. Run `yarn install` from this directory. If you don't have `yarn`, download it.

2. Cypress tests run against your local deployment of datahub. They are dependent on the data inside. There is sample cypress data in this directory. Ideally you want to delete all the data in your local instance before ingesting the cypress data as it will throw off a few tests (like the search tests) if you have data cypress is not expecting. However, most tests will still pass- most visit specific entity pages.

3. Ingest the cypress data! Using datahub cli, run `datahub ingest -c example_to_datahub_rest.yml` and then `datahub ingest -c example_siblings_to_datahub_rest.yml`.

4. Set the port that you want to run your cypress tests against in ./cypress.json. The default is 9002- if you are developing on react locally, you probably want 3000. Do not commit this change to github.

5. Now, start the local cypress server: `npx cypress open`.
