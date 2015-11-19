App.DatasetsController = Ember.Controller.extend({
    urn: null,
    currentName: null,
    urnWatched: false,
    detailview: true,
    previousPage: function(){
        var model = this.get("model");
        if (model && model.data && model.data.page) {
            var currentPage = model.data.page;
            if (currentPage <= 1) {
                return currentPage;
            }
            else {
                return currentPage - 1;
            }
        } else {
            return 1;
        }

    }.property('model.data.page'),
    nextPage: function(){
        var model = this.get("model");
        if (model && model.data && model.data.page) {
            var currentPage = model.data.page;
            var totalPages = model.data.totalPages;
            if (currentPage >= totalPages) {
                return totalPages;
            }
            else {
                return currentPage + 1;
            }
        } else {
            return 1;
        }
    }.property('model.data.page'),
    first: function(){
        var model = this.get("model");
        if (model && model.data && model.data.page) {
            var currentPage = model.data.page;
            if (currentPage <= 1) {
                return true;
            }
            else {
                return false
            }
        } else {
            return false;
        }
    }.property('model.data.page'),
    last: function(){
        var model = this.get("model");
        if (model && model.data && model.data.page) {
            var currentPage = model.data.page;
            var totalPages = model.data.totalPages;
            if (currentPage >= totalPages) {
                return true;
            }
            else {
                return false
            }
        } else {
            return false;
        }
    }.property('model.data.page'),
    getUrnWatchId: function(urn){
        var controller = this;
        var watcherEndpoint = "/api/v1/urn/watch?urn=" + urn
        $.get(watcherEndpoint, function(data){
            if(data.id && data.id !== 0) {
                controller.set('urnWatched', true)
                controller.set('urnWatchedId', data.id)
            } else {
                controller.set('urnWatched', false)
                controller.set('urnWatchedId', 0)
            }
        })
    },
    actions: {
        watchUrn: function(urn) {
            if (!urn)
            {
                urn = "DATASETS_ROOT";
            }
            var _this = this
            var url = "/api/v1/urn/watch"
            var token = $("#csrfToken").val().replace('/', '')
            if(!this.get('urnWatched')) {
                //this.set('urnWatched', false)
                $.ajax({
                    url: url,
                    method: 'POST',
                    header: {
                        'Csrf-Token': token
                    },
                    data: {
                        csrfToken: token,
                        urn: urn,
                        type: 'urn',
                        'notification_type': 'WEEKLY'
                    }
                }).done(function(data, txt, xhr){
                    Notify.toast('You are now watching: ' + urn , 'Success', 'success')
                    _this.set('urnWatched', true)
                    _this.getUrnWatchId(urn);
                }).fail(function(xhr, txt, error){
                    Notify.toast('URN could not be watched', 'Error Watching Urn', 'error')
                })
            } else {
                url += ("/" + this.get('urnWatchedId'))
                url += "?csrfToken=" + token
                $.ajax({
                    url: url,
                    method: 'DELETE',
                    header: {
                        'Csrf-Token': token
                    },
                    dataType: 'json'
                }).done(function(data, txt, xhr){
                    _this.set('urnWatched', false)
                    _this.set('urnWatchedId', 0)
                }).fail(function(xhr, txt, error){
                    Notify.toast('Could not unwatch urn', 'Error', 'error')
                })
            }
        }
    }
});

App.SubpageController = Ember.Controller.extend({
    queryParams: ['urn'],
    urn: null
});

App.DatasetController = Ember.Controller.extend({
    hasProperty: false,
    hasImpacts: false,
    hasSchemas: false,
    hasSamples: false,
    isTable: true,
    isJSON: false,
    isPinot: function(){
        var model = this.get("model");
        if (model)
        {
            if (model.source)
            {
                return model.source.toLowerCase() == 'pinot';
            }
        }
        return false;

    }.property('model.source'),
    isHDFS: function(){
        var model = this.get("model");
        if (model)
        {
            if (model.urn)
            {
                return model.urn.substring(0,7) == 'hdfs://';
            }
        }
        return false;

    }.property('model.urn'),
    isSFDC: function(){
        var model = this.get("model");
        if (model)
        {
            if (model.source)
            {
                return model.source.toLowerCase() == 'salesforce';
            }
        }
        return false;

    }.property('model.source'),
    lineageUrl: function(){
        var model = this.get("model");
        if (model)
        {
            if (model.id)
            {
                return '/lineage/dataset/' + model.id;
            }
        }
        return '';

    }.property('model.id'),
    adjustPanes: function() {
        var hasProperty = this.get('hasProperty')
        var isHDFS = this.get('isHDFS')
        if(hasProperty && !isHDFS) {
            $("#sampletab").css('overflow', 'scroll');
            // Adjust the height
            // Set global adjuster
            var height = ($(window).height() * 0.99) - 185;
            $("#sampletab").css('height', height);
            $(window).resize(function(){
                var height = ($(window).height() * 0.99) - 185;
                $('#sampletab').height(height)
            })
        }
    }.observes('hasProperty', 'isHDFS').on('init'),
    buildJsonView: function(){
        var model = this.get("model");
        var schema = JSON.parse(JSON.stringify(model.schema))
        setTimeout(function() {
            $("#json-viewer").JSONView(schema)
        }, 300)
    },
    actions: {
        setView: function(view) {
            switch(view) {
                case "tabular":
                    this.set('isTable', true)
                    this.set('isJSON', false)
                    break;
                case "json":
                    this.set('isTable', false)
                    this.set('isJSON', true)
                    this.buildJsonView()
                    break;
                default:
                    this.set('isTable', true)
                    this.set('isJSON', false)
            }
        }
    }
});

