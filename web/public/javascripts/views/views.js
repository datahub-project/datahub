App.PropertyView = Ember.View.extend({
    templateName: 'property'
});

App.SchemaView = Ember.View.extend({
    templateName: 'schema'
});

App.SampleView = Ember.View.extend({
  templateName: 'sample'
});

App.ImpactView = Ember.View.extend({
    templateName: 'impact'
});

App.OwnerView = Ember.View.extend({
    templateName: 'owner'
});

App.DependView = Ember.View.extend({
    templateName: 'depend'
});

App.DetailView = Ember.View.extend({
    templateName: 'detail',
    didInsertElement: function() {
        var self = this;
        $.fn.editable.defaults.mode = 'inline';
        $('.xeditable').editable({
            url: function(param) {
                if (param && param.name)
                {
                    var name = param.name;
                    var val = param.value;
                    var metricId = param.pk;
                    var url = '/api/v1/metrics/' + metricId + '/update';
                    var method = 'POST';
                    var token = $("#csrfToken").val().replace('/', '');
                    var data = {"csrfToken": token};
                    data[name] = val;
                    $.ajax({
                        url: url,
                        method: method,
                        headers: {
                            'Csrf-Token': token
                        },
                        dataType: 'json',
                        data: data
                    }).done(function(data, txt, xhr){
                        if(data && data.status && data.status == "success")
                        {
                            console.log('Done.')
                        }
                        else
                        {
                            console.log('Failed.')
                        }
                    }).fail(function(xhr, txt, err){
                        Notify.toast("Failed to update data", "Metric Update Failure", "error")
                    })
                }
            }
        });
    }
});
