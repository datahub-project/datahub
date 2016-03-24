(function (window, $) {
    $('#advsearchtabs a:first').tab("show");
    $('#datasetAdvSearchLink').addClass("active");
    String.prototype.replaceAll = function(target, replacement) {
        return this.split(target).join(replacement);
    };
    window.g_currentCategory = 'Datasets';
    function renderAdvSearchDatasetSources(parent, sources)
        {
            if ((!parent) || (!sources) || sources.length == 0)
                return;

            var content = '';
            for (var i = 0; i < sources.length; i++)
            {
                content += '<label class="checkbox"><input type="checkbox" name="sourceCheckbox" value="';
                content += sources[i] + '"/>' + sources[i] + '</label>';
            }
            parent.append(content);
        }

        $.ui.autocomplete.prototype._renderItem = function( ul, item){
            var term = this.term.split(' ').join('|');
            var re = new RegExp("(" + term + ")", "gi") ;
            var t = item.label.replace(re,"<b><font color='blue'>$1</font></b>");
            return $( "<li></li>" )
                .data( "item.autocomplete", item )
                .append( "<a>" + t + "</a>" )
                .appendTo( ul );
        };

        var maxReturnedResults = 20;
        function split( val ) {
            return val.split( /,\s*/ );
        }

        function extractLast( term ) {
            return split( term ).pop();
        }

        function sortAutocompleteResult(data, term)
        {
            var source = $.ui.autocomplete.filter(data, term);
            var keyword = $.ui.autocomplete.escapeRegex(term);
            if (keyword && keyword.length < 4)
            {
                return source.slice(0, maxReturnedResults);
            }

            var startsWithMatcher = new RegExp("^" + keyword, "i");
            var startsWith = $.grep(source, function(value) {
                return startsWithMatcher.test(value.label || value.value || value);
            });
            var containsMatcher = new RegExp(keyword, "i")
            var contains = $.grep(source, function (value) {
                return containsMatcher.test(value.label || value.value || value);
            });
            var result = startsWith.concat(contains);
            var sorted = result.filter(function(elem, pos) {
                return result.indexOf(elem) == pos;
            });
            return sorted.slice(0, maxReturnedResults);
        }

        function renderAdvSearchDatasetSources(parent, sources)
        {
            if ((!parent) || (!sources) || sources.length == 0)
                return;

            var content = '';
            for (var i = 0; i < sources.length; i++)
            {
                content += '<label class="checkbox"><input type="checkbox" name="sourceCheckbox" value="';
                content += sources[i] + '"/>' + sources[i] + '</label>';
            }
            parent.append(content);
        }

    $(".searchCategory").click(function(e){
        var objs = $(".searchCategory");
        if (objs)
        {
            $.each(objs, function( index, value ) {
                $(objs[index]).parent().removeClass("active");
            });
        }
        window.g_currentCategory = e.target.text;
        updateSearchCategories(e.target.text);
        //$(e.target).parent().addClass( "active" );
        e.preventDefault();
    });

    var datasetSourcesUrl = '/api/v1/advsearch/sources';
        $.get(datasetSourcesUrl, function(data) {
            if (data && data.status == "ok")
            {
                var advSearchSourceObj = $("#advSearchSource");
                if (advSearchSourceObj)
                {
                    if (data.sources)
                    {
                        renderAdvSearchDatasetSources(advSearchSourceObj, data.sources);
                    }
                }
            }
        });

        $.get('/api/v1/autocomplete/search', function(data){
            $('#searchInput').autocomplete({
                source: function( req, res ) {
                    var results = $.ui.autocomplete.filter(data.source, extractLast( req.term ));
                    res(results.slice(0,maxReturnedResults));
                },
                focus: function() {
                    return false;
                },
                select: function( event, ui ) {
                    var terms = split( this.value );
                    terms.pop();
                    terms.push( ui.item.value );
                    terms.push( "" );
                    this.value = terms.join( ", " );
                    return false;
            }

        });

        });

        $.get('/api/v1/advsearch/scopes', function(data){
            $(".scopeInput").autocomplete({
                minLength: 0,
                source: function( req, res ) {
                    var results = $.ui.autocomplete.filter(data.scopes, extractLast( req.term ));
                    res(results.slice(0,maxReturnedResults));
                },
                focus: function() {
                    return false;
                },
                select: function( event, ui ) {
                    var terms = split( this.value );
                    terms.pop();
                    terms.push( ui.item.value );
                    terms.push( "" );
                    this.value = terms.join( ", " );
                    return false;
                }
            });
        });

        $.get('/api/v1/advsearch/tables', function(data){
            $(".tableInput").autocomplete({
                minLength: 0,
                source: function( req, res ) {
                    var results = $.ui.autocomplete.filter(data.tables, extractLast( req.term ));
                    res(results.slice(0,maxReturnedResults));
                },
                focus: function() {
                    return false;
                },
                select: function( event, ui ) {
                    var terms = split( this.value );
                    terms.pop();
                    terms.push( ui.item.value );
                    terms.push( "" );
                    this.value = terms.join( ", " );
                    return false;
                }
            });
        });

        $.get('/api/v1/advsearch/fields', function(data){
            $(".fieldInput").autocomplete({
                minLength: 0,
                source: function( req, res ) {
                    var results = $.ui.autocomplete.filter(data.fields, extractLast( req.term ));
                    res(results.slice(0,maxReturnedResults));
                },
                focus: function() {
                    return false;
                },
                select: function( event, ui ) {
                    var terms = split( this.value );
                    terms.pop();
                    terms.push( ui.item.value );
                    terms.push( "" );
                    this.value = terms.join( ", " );
                    return false;
                }
            });
        });

        $.get('/api/v1/advsearch/appcodes', function(data){
            $(".appcodeInput").autocomplete({
                minLength: 0,
                source: function( req, res ) {
                    var results = $.ui.autocomplete.filter(data.appcodes, extractLast( req.term ));
                    res(results.slice(0,maxReturnedResults));
                },
                focus: function() {
                    return false;
                },
                select: function( event, ui ) {
                    var terms = split( this.value );
                    terms.pop();
                    terms.push( ui.item.value );
                    terms.push( "" );
                    this.value = terms.join( ", " );
                    return false;
                }
            });
        });

        $.get('/api/v1/advsearch/flowNames', function(data){
            $(".flowInput").autocomplete({
                minLength: 0,
                source: function( req, res ) {
                    var results = $.ui.autocomplete.filter(data.flowNames, extractLast( req.term ));
                    res(results.slice(0,maxReturnedResults));
                },
                focus: function() {
                    return false;
                },
                select: function( event, ui ) {
                    var terms = split( this.value );
                    terms.pop();
                    terms.push( ui.item.value );
                    terms.push( "" );
                    this.value = terms.join( ", " );
                    return false;
                }
            });
        });

        $.get('/api/v1/advsearch/jobNames', function(data){
            $(".jobInput").autocomplete({
                minLength: 0,
                source: function( req, res ) {
                    var results = $.ui.autocomplete.filter(data.jobNames, extractLast( req.term ));
                    res(results.slice(0,maxReturnedResults));
                },
                focus: function() {
                    return false;
                },
                select: function( event, ui ) {
                    var terms = split( this.value );
                    terms.pop();
                    terms.push( ui.item.value );
                    terms.push( "" );
                    this.value = terms.join( ", " );
                    return false;
                }
            });
        });

        $.get('/api/v1/advsearch/dashboards', function(data){
            $(".dashInput").autocomplete({
                minLength: 0,
                source: function( req, res ) {
                    var results = $.ui.autocomplete.filter(data.dashboardNames, extractLast( req.term ));
                    res(results.slice(0,maxReturnedResults));
                },
                focus: function() {
                    return false;
                },
                select: function( event, ui ) {
                    var terms = split( this.value );
                    terms.pop();
                    terms.push( ui.item.value );
                    terms.push( "" );
                    this.value = terms.join( ", " );
                    return false;
                }
            });
        });

        $.get('/api/v1/advsearch/metricGroups', function(data){
            $(".groupInput").autocomplete({
                minLength: 0,
                source: function( req, res ) {
                    var results = $.ui.autocomplete.filter(data.metricGroups, extractLast( req.term ));
                    res(results.slice(0,maxReturnedResults));
                },
                focus: function() {
                    return false;
                },
                select: function( event, ui ) {
                    var terms = split( this.value );
                    terms.pop();
                    terms.push( ui.item.value );
                    terms.push( "" );
                    this.value = terms.join( ", " );
                    return false;
                }
            });
        });

        $.get('/api/v1/advsearch/metricCategories', function(data){
            $(".categoryInput").autocomplete({
                minLength: 0,
                source: function( req, res ) {
                    var results = $.ui.autocomplete.filter(data.metricCategories, extractLast( req.term ));
                    res(results.slice(0,maxReturnedResults));
                },
                focus: function() {
                    return false;
                },
                select: function( event, ui ) {
                    var terms = split( this.value );
                    terms.pop();
                    terms.push( ui.item.value );
                    terms.push( "" );
                    this.value = terms.join( ", " );
                    return false;
                }
            });
        });

        $.get('/api/v1/advsearch/metricNames', function(data){
            $(".metricnameInput").autocomplete({
                minLength: 0,
                source: function( req, res ) {
                    var result = [];
                    if (data && data.metricNames && req.term)
                    {
                        result = sortAutocompleteResult(data.metricNames, req.term);
                    }
                    return res(result);
                },
                focus: function() {
                    return false;
                },
                select: function( event, ui ) {
                    var terms = split( this.value );
                    terms.pop();
                    terms.push( ui.item.value );
                    terms.push( "" );
                    this.value = terms.join( ", " );
                    return false;
                }
            });
        });

        $( "#scopeInInput" ).blur(function() {
            $.get('/api/v1/advsearch/tables', {scopes: $( "#scopeInInput").val()}, function(data){
                $(".tableInput").autocomplete({
                    minLength: 0,
                    source: function( req, res ) {
                        var results = $.ui.autocomplete.filter(data.tables, extractLast( req.term ));
                        res(results.slice(0,maxReturnedResults));
                    },
                    focus: function() {
                        return false;
                    },
                    select: function( event, ui ) {
                        var terms = split( this.value );
                        terms.pop();
                        terms.push( ui.item.value );
                        terms.push( "" );
                        this.value = terms.join( ", " );
                        return false;
                    }
                });
            });
        });

        $( "#tableInInput" ).blur(function() {
            $.get('/api/v1/advsearch/fields', {tables: $( "#tableInInput").val()}, function(data){
                $(".fieldInput").autocomplete({
                    minLength: 0,
                    source: function( req, res ) {
                        var results = $.ui.autocomplete.filter(data.fields, extractLast( req.term ));
                        res(results.slice(0,maxReturnedResults));
                    },
                    focus: function() {
                        return false;
                    },
                    select: function( event, ui ) {
                        var terms = split( this.value );
                        terms.pop();
                        terms.push( ui.item.value );
                        terms.push( "" );
                        this.value = terms.join( ", " );
                        return false;
                    }
                });
            });
        });

        $('#searchBtn').click(function(){
            var inputObj = $('#searchInput');
            if (inputObj)
            {
                var keyword = inputObj.val();
                if (keyword)
                {
                    window.location = '/#/search?keywords=' + btoa(keyword) +
                        '&category=' + window.g_currentCategory + '&source=default&page=1'
                }
            }
        });

        function advSearchForDataset()
        {
            var empty = true;
            var scopeInInputObj = $('#scopeInInput');
            var scopeIn = '';
            if (scopeInInputObj)
            {
                scopeIn = scopeInInputObj.val();
                if (scopeIn)
                {
                    empty = false;
                }
            }
            var scopeNotInInputObj = $('#scopeNotInInput');
            var scopeNotIn = '';
            if (scopeNotInInputObj)
            {
                scopeNotIn = scopeNotInInputObj.val();
                if (scopeNotIn)
                {
                    empty = false;
                }
            }
            var tableInInputObj = $('#tableInInput');
            var tableIn = '';
            if (tableInInputObj)
            {
                tableIn = tableInInputObj.val();
                if (tableIn)
                {
                    empty = false;
                }
            }
            var tableNotInInputObj = $('#tableNotInInput');
            var tableNotIn = '';
            if (tableNotInInputObj)
            {
                tableNotIn = tableNotInInputObj.val();
                if (tableNotIn)
                {
                    empty = false;
                }
            }
            var fieldAnyInputObj = $('#fieldAnyInput');
            var fieldAny = '';
            if (fieldAnyInputObj)
            {
                fieldAny = fieldAnyInputObj.val();
                if (fieldAny)
                {
                    empty = false;
                }
            }
            var fieldAllInputObj = $('#fieldAllInput');
            var fieldAll = '';
            if (fieldAllInputObj)
            {
                fieldAll = fieldAllInputObj.val();
                if (fieldAll)
                {
                    empty = false;
                }
            }
            var fieldNotInInputObj = $('#fieldNotInInput');
            var fieldNotIn = '';
            if (fieldNotInInputObj)
            {
                fieldNotIn = fieldNotInInputObj.val();
                if (fieldNotIn)
                {
                    empty = false;
                }
            }
            var commentsInputObj = $('#commentsInput');
            var comments = '';
            if (commentsInputObj)
            {
                comments = commentsInputObj.val();
                if (comments)
                {
                    empty = false;
                }
            }
            var sources = '';
            $('input[name="sourceCheckbox"]:checked').each(function() {
                sources += this.value + ','
            });
            sources = sources.substring(0, sources.length-1);
            if (sources)
            {
                empty = false;
            }
            if (empty)
            {
                return;
            }

            var advSearchOpts = {};
            advSearchOpts.category = 'Dataset';
            advSearchOpts.scope = {'in': scopeIn, 'not': scopeNotIn};
            advSearchOpts.table = {'in': tableIn, 'not': tableNotIn};
            advSearchOpts.fields = {'any': fieldAny, 'all': fieldAll, 'not': fieldNotIn};
            advSearchOpts.comments = comments;
            advSearchOpts.sources = sources;
            window.location = "/#/advsearch/?query=" + btoa(JSON.stringify(advSearchOpts)) + '&page=1';
        }

        function advSearchForMetric()
        {
            var empty = true;
            var dashInInputObj = $('#dashInInput');
            var dashIn = '';
            if (dashInInputObj)
            {
                dashIn = dashInInputObj.val();
                if (dashIn)
                {
                    empty = false;
                }
            }
            var dashNotInInputObj = $('#dashNotInInput');
            var dashNotIn = '';
            if (dashNotInInputObj)
            {
                dashNotIn = dashNotInInputObj.val();
                if (dashNotIn)
                {
                    empty = false;
                }
            }
            var groupInInputObj = $('#groupInInput');
            var groupIn = '';
            if (groupInInputObj)
            {
                groupIn = groupInInputObj.val();
                if (groupIn)
                {
                    empty = false;
                }
            }
            var groupNotInInputObj = $('#groupNotInInput');
            var groupNotIn = '';
            if (groupNotInInputObj)
            {
                groupNotIn = groupNotInInputObj.val();
                if (groupNotIn)
                {
                    empty = false;
                }
            }
            var categoryInInputObj = $('#categoryInInput');
            var categoryIn = '';
            if (categoryInInputObj)
            {
                categoryIn = categoryInInputObj.val();
                if (categoryIn)
                {
                    empty = false;
                }
            }
            var categoryNotInInputObj = $('#categoryNotInInput');
            var categoryNotIn = '';
            if (categoryNotInInputObj)
            {
                categoryNotIn = categoryNotInInputObj.val();
                if (categoryNotIn)
                {
                    empty = false;
                }
            }
            var metricnameInInputObj = $('#metricnameInInput');
            var metricnameIn = '';
            if (metricnameInInputObj)
            {
                metricnameIn = metricnameInInputObj.val();
                if (metricnameIn)
                {
                    empty = false;
                }
            }
            var metricnameNotInInputObj = $('#metricnameNotInInput');
            var metricnameNotIn = '';
            if (metricnameNotInInputObj)
            {
                metricnameNotIn = metricnameNotInInputObj.val();
                if (metricnameNotIn)
                {
                    empty = false;
                }
            }

            if (empty)
            {
                return;
            }

            var advSearchOpts = {};
            advSearchOpts.category = 'Metric';
            advSearchOpts.dashboard = {'in': dashIn, 'not': dashNotIn};
            advSearchOpts.group = {'in': groupIn, 'not': groupNotIn};
            advSearchOpts.cat = {'in': categoryIn, 'not': categoryNotIn};
            advSearchOpts.metric = {'in': metricnameIn, 'not': metricnameNotIn};
            window.location = "/#/advsearch/?query=" + btoa(JSON.stringify(advSearchOpts)) + '&page=1';
        }

        function advSearchForFlow()
        {
            var empty = true;
            var appcodeInInputObj = $('#appcodeInInput');
            var appcodeIn = '';
            if (appcodeInInputObj)
            {
                appcodeIn = appcodeInInputObj.val();
                if (appcodeIn)
                {
                    empty = false;
                }
            }
            var appcodeNotInInputObj = $('#appcodeNotInInput');
            var appcodeNotIn = '';
            if (appcodeNotInInputObj)
            {
                appcodeNotIn = appcodeNotInInputObj.val();
                if (appcodeNotIn)
                {
                    empty = false;
                }
            }
            var flowInInputObj = $('#flowInInput');
            var flowIn = '';
            if (flowInInputObj)
            {
                flowIn = flowInInputObj.val();
                if (flowIn)
                {
                    empty = false;
                }
            }
            var flowNotInInputObj = $('#flowNotInInput');
            var flowNotIn = '';
            if (flowNotInInputObj)
            {
                flowNotIn = flowNotInInputObj.val();
                if (flowNotIn)
                {
                    empty = false;
                }
            }
            var jobInInputObj = $('#jobInInput');
            var jobIn = '';
            if (jobInInputObj)
            {
                jobIn = jobInInputObj.val();
                if (jobIn)
                {
                    empty = false;
                }
            }
            var jobNotInInputObj = $('#jobNotInInput');
            var jobNotIn = '';
            if (jobNotInInputObj)
            {
                jobNotIn = jobNotInInputObj.val();
                if (jobNotIn)
                {
                    empty = false;
                }
            }

            if (empty)
            {
                return;
            }

            var advSearchOpts = {};
            advSearchOpts.category = 'Flow';
            advSearchOpts.appcode = {'in': appcodeIn, 'not': appcodeNotIn};
            advSearchOpts.flow = {'in': flowIn, 'not': flowNotIn};
            advSearchOpts.job = {'in': jobIn, 'not': jobNotIn};
            window.location = "/#/advsearch/?query=" + btoa(JSON.stringify(advSearchOpts)) + '&page=1';
        }

        $('#advSearchBtn').click(function(){
            var obj = $("#advsearchtabs").find(".active")
            if (obj)
            {
                var text = obj.text();
                if (text == 'Datasets')
                {
                    advSearchForDataset();
                }
                else if (text == 'Metrics')
                {
                    advSearchForMetric();
                }
                else
                {
                    advSearchForFlow();
                }
            }
        });

        $('#advSearchResetBtn').click(function(){
            var scopeInInputObj = $('#scopeInInput');
            if (scopeInInputObj)
            {
                scopeInInputObj.val('');
            }
            var scopeNotInInputObj = $('#scopeNotInInput');
            if (scopeNotInInputObj)
            {
                scopeNotInInputObj.val('');
            }
            var tableInInputObj = $('#tableInInput');
            if (tableInInputObj)
            {
                tableInInputObj.val('');
            }
            var tableNotInInputObj = $('#tableNotInInput');
            if (tableNotInInputObj)
            {
                tableNotInInputObj.val('');
            }
            var fieldAnyInputObj = $('#fieldAnyInput');
            if (fieldAnyInputObj)
            {
                fieldAnyInputObj.val('');
            }
            var fieldAllInputObj = $('#fieldAllInput');
            if (fieldAllInputObj)
            {
                fieldAllInputObj.val('');
            }
            var fieldNotInInputObj = $('#fieldNotInInput');
            if (fieldNotInInputObj)
            {
                fieldNotInInputObj.val('');
            }
            var commentsInputObj = $('#commentsInput');
            if (commentsInputObj)
            {
                commentsInputObj.val('');
            }
            $('input[name="sourceCheckbox"]:checked').each(function() {
                this.checked = false;
            });
            var appcodeInInputObj = $('#appcodeInInput');
            if (appcodeInInputObj)
            {
                appcodeInInputObj.val('');
            }
            var appcodeNotInInputObj = $('#appcodeNotInInput');
            if (appcodeNotInInputObj)
            {
                appcodeNotInInputObj.val('');
            }
            var flowInInputObj = $('#flowInInput');
            if (flowInInputObj)
            {
                flowInInputObj.val('');
            }
            var flowNotInInputObj = $('#flowNotInInput');
            if (flowNotInInputObj)
            {
                flowNotInInputObj.val('');
            }
            var jobInInputObj = $('#jobInInput');
            if (jobInInputObj)
            {
                jobInInputObj.val('');
            }
            var jobNotInInputObj = $('#jobNotInInput');
            if (jobNotInInputObj)
            {
                jobNotInInputObj.val('');
            }
            var dashboardInInputObj = $('#dashInInput');
            if (dashboardInInputObj)
            {
                dashboardInInputObj.val('');
            }
            var dashboardNotInInputObj = $('#dashNotInInput');
            if (dashboardNotInInputObj)
            {
                dashboardNotInInputObj.val('');
            }
            var groupInInputObj = $('#groupInInput');
            if (groupInInputObj)
            {
                groupInInputObj.val('');
            }
            var groupNotInInputObj = $('#groupNotInInput');
            if (groupNotInInputObj)
            {
                groupNotInInputObj.val('');
            }
            var categoryInInputObj = $('#categoryInInput');
            if (categoryInInputObj)
            {
                categoryInInputObj.val('');
            }
            var categoryNotInInputObj = $('#categoryNotInInput');
            if (categoryNotInInputObj)
            {
                categoryNotInInputObj.val('');
            }
            var metricInInputObj = $('#metricnameInInput');
            if (metricInInputObj)
            {
                metricInInputObj.val('');
            }
            var metricNotInInputObj = $('#metricnameNotInInput');
            if (metricNotInInputObj)
            {
                metricNotInInputObj.val('');
            }
        });

})(window, jQuery)
