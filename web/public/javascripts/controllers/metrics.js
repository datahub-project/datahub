App.MetricController = Ember.Controller.extend({
    isEdit: false,
    updateLoading: false,
    lineageUrl: function(){
        var model = this.get("model");
        if (model)
        {
            if (model.refID)
            {
                var id = parseInt(model.refID);
                if (id > 0)
                {
                    return '/lineage/metric/' + model.refID;
                }
            }
        }
        return '';

    }.property('model.refID'),
    showLineage: function(){
        var model = this.get("model");
        if (model)
        {
            if (model.refID)
            {
                var id = parseInt(model.refID);
                if (id > 0)
                {
                    return true;
                }
            }
        }
        return false;

    }.property('model.refID'),
    actions: {
        editMode: function() {
            this.set('isEdit', true)
        },
        cancelEditMode: function() {
            this.set('isEdit', false)
        },
        update: function(){
            var model = this.get("model")
            var url = '/api/v1/metrics/' + model.id + '/update'
            var token = $("#csrfToken").val().replace('/', '')
            var _this = this
            var data = JSON.parse(JSON.stringify(model))
            this.set('updateLoading', true)
            data.token = token
            $.ajax({
                url: url,
                method: 'POST',
                //contentType: 'application/json',
                headers: {
                    'Csrf-Token': token
                },
                dataType: 'json',
                //data: JSON.stringify(data)
                data: data
            }).done(function(data, txt, xhr){
                _this.set('isEdit', false)
                _this.set('updateLoading', false)
            }).fail(function(xhr, txt, err){
                Notify.toast('Could not update.', 'Update Metric', 'Error')
                _this.set('updateLoading', false)
            })
        }
    }
});

App.MetricsController = Ember.Controller.extend({
    dashboard: null,
    group: null,
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
        var watcherEndpoint = "/api/v1/urn/watch?urn=" + urn;
        $.get(watcherEndpoint, function(data){
            if(data.id && data.id !== 0) {
                controller.set('urnWatched', true);
                controller.set('urnWatchedId', data.id);
            } else {
                controller.set('urnWatched', false);
                controller.set('urnWatchedId', 0);
            }
        })
    },
    actions: {
        watchUrn: function(urn) {
            if (!urn)
            {
                urn = "METRICS_ROOT";
            }
            var _this = this;
            var url = "/api/v1/urn/watch";
            var token = $("#csrfToken").val().replace('/', '');
            if(!this.get('urnWatched')) {
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
