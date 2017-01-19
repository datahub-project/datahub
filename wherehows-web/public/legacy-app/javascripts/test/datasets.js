function testPagedDatasets(assert)
{
    var done = assert.async();
    var page = 1;
    var size = 10;
    var url = '/api/v1/datasets?size=' + size + '&page=' + page;

    $.get( url, function( result ) {
        assert.equal(result.status, 'ok', "Called getPagedDatasets check status");
        assert.equal(result.data.page, '1', "Called getPagedDatasets check if return correct page.");
        assert.ok(result.data.count > 0, "Total " + result.data.count + " datasets returned.");
        done();
        var dataset = result.data.datasets[0];
        testDatasetSchema(assert, dataset);
        testDatasetOwners(assert, dataset);
        if (dataset.source != 'Hive')
        {
            testDatasetSample(assert, dataset);
        }
        testDatasetImpacts(assert, dataset);
        testDatasetDepends(assert, dataset);
        testDatasetRefrences(assert, dataset);
        testDatasetAvailability(assert, dataset);
    });
}

function testDatasetSchema(assert, dataset)
{
    if (dataset && dataset.id && dataset.id > 0)
    {
        var done = assert.async();
        var url = '/api/v1/datasets/' + dataset.id + '/columns';

        $.get( url, function( result ) {
            assert.equal(result.status, 'ok', "Dataset id = " + dataset.id + " schema");
            if (result.columns && result.columns.length > 0)
            {
                assert.ok(true, "Tabular schema view is available for Dataset id = " + dataset.id);
            }
            else
            {
                assert.ok(true, "Tabular schema view is not available for Dataset id = " + dataset.id + ", but JSON view is available.");
            }
            done();
        });
    }
}

function testDatasetOwners(assert, dataset)
{
    if (dataset && dataset.id && dataset.id > 0)
    {
        var done = assert.async();
        var url = '/api/v1/datasets/' + dataset.id + '/owners';

        $.get( url, function( result ) {
            assert.equal(result.status, 'ok', "Dataset id = " + dataset.id + " ownership");
            if (result.owners && result.owners.length > 0)
            {
                assert.ok(true, "Owner information is available for Dataset id = " + dataset.id);
            }
            else
            {
                assert.ok(true, "Owner information is not available for Dataset id = " + dataset.id);
            }
            done();
        });
    }
}

function testDatasetSample(assert, dataset)
{
    if (dataset && dataset.id && dataset.id > 0)
    {
        var done = assert.async();
        var url = '/api/v1/datasets/' + dataset.id + '/sample';

        $.get( url, function( result ) {
            assert.equal(result.status, 'ok', "Dataset id = " + dataset.id + " sample data.");
            if (result.sampleData && result.sampleData.length > 0)
            {
                assert.ok(true, "Sample data is available for Dataset id = " + dataset.id);
            }
            else
            {
                assert.ok(true, "Sample data is not available for Dataset id = " + dataset.id);
            }
            done();
        });
    }
}

function testDatasetImpacts(assert, dataset)
{
    if (dataset && dataset.id && dataset.id > 0)
    {
        var done = assert.async();
        var url = '/api/v1/datasets/' + dataset.id + '/impacts';

        $.get( url, function( result ) {
            assert.equal(result.status, 'ok', "Dataset id = " + dataset.id + " down stream impact analysis");
            if (result.impacts && result.impacts.length > 0)
            {
                assert.ok(true, "Down stream impact analysis is available for Dataset id = " + dataset.id);
            }
            else
            {
                assert.ok(true, "Down stream impact analysis is not available for Dataset id = " + dataset.id);
            }
            done();
        });
    }
}

function testDatasetDepends(assert, dataset)
{
    if (dataset && dataset.id && dataset.id > 0)
    {
        var done = assert.async();
        var url = '/api/v1/datasets/' + dataset.id + '/depends';

        $.get( url, function( result ) {
            assert.equal(result.status, 'ok', "Dataset id = " + dataset.id + " depends");
            if (result.depends && result.depends.length > 0)
            {
                assert.ok(true, "Depends view is available for Dataset id = " + dataset.id);
            }
            else
            {
                assert.ok(true, "Depends view is not available for Dataset id = " + dataset.id);
            }
            done();
        });
    }
}

function testDatasetRefrences(assert, dataset)
{
    if (dataset && dataset.id && dataset.id > 0)
    {
        var done = assert.async();
        var url = '/api/v1/datasets/' + dataset.id + '/references';

        $.get( url, function( result ) {
            assert.equal(result.status, 'ok', "Dataset id = " + dataset.id + " references");
            if (result.references && result.references.length > 0)
            {
                assert.ok(true, "References view is available for Dataset id = " + dataset.id);
            }
            else
            {
                assert.ok(true, "References view is not available for Dataset id = " + dataset.id);
            }
            done();
        });
    }
}

function testDatasetAvailability(assert, dataset)
{
    if (dataset && dataset.id && dataset.id > 0)
    {
        var done = assert.async();
        var url = '/api/v1/datasets/' + dataset.id + '/access';

        $.get( url, function( result ) {
            assert.equal(result.status, 'ok', "Dataset id = " + dataset.id + " availability");
            if (result.references && result.references.length > 0)
            {
                assert.ok(true, "Dataset availability is available for id = " + dataset.id);
            }
            else
            {
                assert.ok(true, "Dataset availability is not available for id = " + dataset.id);
            }
            done();
        });
    }
}

QUnit.test( "datasets test", function( assert ) {
    testPagedDatasets(assert);
});
