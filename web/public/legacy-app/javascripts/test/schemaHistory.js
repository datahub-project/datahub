function testPagedSchemaHistory(assert)
{
    var done = assert.async();
    var page = 1;
    var size = 10;
    var url = '/api/v1/schemaHistory/datasets?size=' + size + '&page=' + page;

    $.get( url, function( result ) {
        assert.equal(result.status, 'ok', "Called getPagedSchemaHistory check status");
        assert.equal(result.data.page, '1', "Called getPagedSchemaHistory check if return correct page.");
        assert.ok(result.data.count > 0, "Total " + result.data.count + " datasets schema hisotry returned.");
        done();
    });
}

QUnit.test( "schema history test", function( assert ) {
    testPagedSchemaHistory(assert);
});
