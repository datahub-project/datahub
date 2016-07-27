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
    currentVersion:'0',
    latestVersion:'0',
    ownerTypes: [],
    userTypes: [{name:"Corporate User", value: "urn:li:corpuser"}, {name:"Group User", value: "urn:li:griduser"}],
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
    schemaHistoryUrl: function(){
        var model = this.get("model");
        if (model)
        {
            if (model.id)
            {
                return '/schemaHistory#/schemas/' + model.id;
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
        var schema = JSON.parse(model.schema)
        setTimeout(function() {
            $("#json-viewer").JSONView(schema)
        }, 500);
    },
    refreshVersions: function(dbId) {
        _this = this;
        var model = this.get("model");
        if (!model || !model.id)
        {
            return;
        }
        var versionUrl = 'api/v1/datasets/' + model.id + "/versions/db/" + dbId;
        $.get(versionUrl, function(data) {
            if (data && data.status == "ok" && data.versions && data.versions.length > 0) {
                _this.set("hasversions", true);
                _this.set("versions", data.versions);
                _this.set("latestVersion", data.versions[0]);
                _this.changeVersion(data.versions[0]);
            }
            else
            {
                _this.set("hasversions", false);
                _this.set("currentVersion", '0');
                _this.set("latestVersion", '0');
            }
        });
    },
    changeVersion: function(version) {
        _this = this;
        var currentVersion = _this.get('currentVersion');
        var latestVersion = _this.get('latestVersion');
        if (currentVersion == version)
        {
            return;
        }
        var objs = $('.version-btn');
        if (objs && objs.length > 0)
        {
            for(var i = 0; i < objs.length; i++)
            {
                $(objs[i]).removeClass('btn-default');
                $(objs[i]).removeClass('btn-primary');
                if (version == $(objs[i]).attr('data-value'))
                {
                    $(objs[i]).addClass('btn-primary');
                }
                else
                {
                    $(objs[i]).addClass('btn-default');
                }
            }
        }
        var model = this.get("model");
        if (version != latestVersion)
        {
            if (!model || !model.id)
            {
                return;
            }
            _this.set('hasSchemas', false);
            var schemaUrl = "/api/v1/datasets/" + model.id + "/schema/" + version;
            $.get(schemaUrl, function(data) {
                if (data && data.status == "ok"){
                    setTimeout(function() {
                        $("#json-viewer").JSONView(JSON.parse(data.schema_text))
                    }, 500);
                }
            });
        }
        else
        {
            if (_this.schemas)
            {
                _this.set('hasSchemas', true);
            }
            else
            {
                _this.buildJsonView();
            }
        }

        _this.set('currentVersion', version);
    },
    actions: {
        setView: function(view) {
            switch(view) {
                case "tabular":
                    this.set('isTable', true);
                    this.set('isJSON', false);
                    $('#json-viewer').hide();
                    $('#json-table').show();
                    break;
                case "json":
                    this.set('isTable', false);
                    this.set('isJSON', true);
                    this.buildJsonView();
                    $('#json-table').hide();
                    $('#json-viewer').show();
                    break;
                default:
                    this.set('isTable', true);
                    this.set('isJSON', false);
                    $('#json-viewer').hide();
                    $('#json-table').show();
            }
        },
        addOwner: function(data) {
            var owners = data;
            var currentUser = this.get("currentUser");
            var addedOwner = {"userName":"Owner","email":null,"name":"","isGroup":false,
                "namespace":"urn:li:griduser","type":"Producer","subType":null,"sortId":0};
            var userEntitiesSource = this.get("userEntitiesSource");
            var userEntitiesMaps = this.get("userEntitiesMaps");
            var exist = false;
            if (owners && owners.length > 0)
            {
                owners.forEach(function(owner){
                    if(owner.userName == addedOwner.userName)
                    {
                        exist = true;
                    }
                });
            }
            var controller = this;
            if (!exist)
            {
                owners.unshiftObject(addedOwner);
                setTimeout(function(){
                    setOwnerNameAutocomplete(controller)
                }, 500);
            }
            else
            {
                console.log("The owner is already exist");
            }
        },
        removeOwner: function(owners, owner) {
            if (owners && owner)
            {
                owners.removeObject(owner);
            }
        },
        updateOwners: function(owners) {
            _this = this;
            var showMsg = this.get("showMsg");
            if (showMsg)
            {
                return;
            }
            var model = this.get("model");
            if (!model || !model.id)
            {
                return;
            }
            var url = "/api/v1/datasets/" + model.id + "/owners";
            var token = $("#csrfToken").val().replace('/', '');
            $.ajax({
                url: url,
                method: 'POST',
                header: {
                    'Csrf-Token': token
                },
                data: {
                    csrfToken: token,
                    owners: JSON.stringify(owners)
                }
            }).done(function(data, txt, xhr){
                if (data.status == "success")
                {
                    _this.set('showMsg', true);
                    _this.set('alertType', "alert-success");
                    _this.set('ownerMessage', "Ownership successfully updated.");
                }
                else
                {
                    _this.set('showMsg', true);
                    _this.set('alertType', "alert-danger");
                    _this.set('ownerMessage', "Ownership update failed.");
                }

            }).fail(function(xhr, txt, error){
                _this.set('showMsg', true);
                _this.set('alertType', "alert-danger");
                _this.set('ownerMessage', "Ownership update failed.");
            })
        },
        updateVersion: function(version) {
            this.changeVersion(version);
        },
        updateInstance: function(instance) {
            _this = this;
            var currentInstance = _this.get('currentInstance');
            var latestInstance = _this.get('latestInstance');
            if (currentInstance == instance.dbId)
            {
                return;
            }
            var objs = $('.instance-btn');
            if (objs && objs.length > 0)
            {
                for(var i = 0; i < objs.length; i++)
                {
                    $(objs[i]).removeClass('btn-default');
                    $(objs[i]).removeClass('btn-primary');

                    if (instance.dbCode == $(objs[i]).attr('data-value'))
                    {
                        $(objs[i]).addClass('btn-primary');
                    }
                    else
                    {
                        $(objs[i]).addClass('btn-default');
                    }
                }
            }

            _this.set('currentInstance', instance.dbId);
            _this.refreshVersions(instance.dbId);
        }
    }
});

