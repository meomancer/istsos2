/*
 * File: app/view/WaterDIschargeEditor.js
 *
 * This file was generated by Ext Designer version 1.2.3.
 * http://www.sencha.com/products/designer/
 *
 * This file will be generated the first time you export.
 *
 * You should implement event handling and custom methods in this
 * class.
 */

Ext.define('istsos.view.WaterDIschargeEditor', {
    extend: 'istsos.view.ui.WaterDIschargeEditor',
    alias: 'widget.waterdischargeeditor',

    initComponent: function() {
        var me = this;
        var ratingCurveStore = Ext.create('istsos.store.RatingCurve');
        Ext.define('ratingCurveModel', {
            extend: 'Ext.data.Model',
            fields: [
                {
                    name: 'from',
                    type: 'date'
                },
                {
                    name: 'to',
                    type: 'date'
                },
                {
                    name: 'low_val',
                    type: 'float'
                },
                {
                    name: 'up_val',
                    type: 'float'
                },
                {
                    name: 'A',
                    type: 'float'
                },
                {
                    name: 'B',
                    type: 'float'
                },
                {
                    name: 'C',
                    type: 'float'
                },
                {
                    name: 'K',
                    type: 'float'
                }
            ]
        });

        // Store for combo listing virtual procedures
        var plist = Ext.create('istsos.store.vplist');
        plist.getProxy().url = Ext.String.format(
            '{0}/istsos/services/{1}/virtualprocedures/operations/getlist',
            wa.url, this.istService
        );

        me.callParent(arguments);

        // When virtual procedure is selected in combo load rating curve grid
        Ext.getCmp('vpcmbplist').on("select",function(combo, record, index, eOpts){
            this.loadRatingCurve(record[0].get('name'));
        },this);

        // *****************************************
        //           RATING CURVE EDITOR
        // *****************************************

        var ratingCurveGrid = Ext.getCmp('vpgridratingcurve');

        // Add row on rating curve grid @ end
        Ext.getCmp('vpbtnaddrc').on('click',function(){
            var row = this.store.getCount();
            var r = Ext.create('ratingCurveModel');
            if (row>0){
                var previous = this.store.getAt(row-1);
                r.set("from",previous.get("to"));
            }
            this.store.insert(row, r);
            if (row>0){
                this.editingPlugin.startEditByPosition({row: row, column: 1});
            }
        },ratingCurveGrid);

        // Add row on rating curve grid above selection
        Ext.getCmp('vpbtnaddbelowrc').on('click',function(){
            var r = Ext.create('ratingCurveModel');
            var sm = this.getSelectionModel();
            var previous = sm.selected.items[0];
            r.set("from",previous.get("to"));
            var row = this.store.indexOf(previous)+1;
            this.store.insert(row, r);
            this.editingPlugin.startEditByPosition({row: row, column: 0});
        },ratingCurveGrid);

        // Add row on rating curve grid below selection
        Ext.getCmp('vpbtnaddaboverc').on('click',function(){
            var r = Ext.create('ratingCurveModel');
            var sm = this.getSelectionModel();
            var firstSelection = sm.selected.items[0];
            var row = this.store.indexOf(firstSelection);
            this.store.insert(row, r);
            this.editingPlugin.startEditByPosition({row: row, column: 0});
        },ratingCurveGrid);

        // Remove selected row from rating curve grid
        Ext.getCmp('vpbtnremoverc').on('click',function(){
            var r = Ext.create('ratingCurveModel');
            var sm = this.getSelectionModel();
            var firstSelection = sm.selected.items[0];
            this.store.remove(sm.selected.items[0]);
            Ext.getCmp('vpbtnaddbelowrc').disable();
            Ext.getCmp('vpbtnaddaboverc').disable();
            Ext.getCmp('vpbtnremoverc').disable();
        },ratingCurveGrid);

        // Enable buttons when grid rows selected
        ratingCurveGrid.getSelectionModel().on("select",function(){
            Ext.getCmp('vpbtnaddbelowrc').enable();
            Ext.getCmp('vpbtnaddaboverc').enable();
            Ext.getCmp('vpbtnremoverc').enable();
        });

        // Enable buttons when grid rows deselected
        ratingCurveGrid.getSelectionModel().on("deselect",function(){
            Ext.getCmp('vpbtnaddbelowrc').disable();
            Ext.getCmp('vpbtnaddaboverc').disable();
            Ext.getCmp('vpbtnremoverc').disable();
        });

        Ext.getCmp('vpbtnsaverc').on('click',function(){

            try{
                this.validateRatingCurve();
            }catch (e){
                Ext.Msg.alert('Validation error', e);
                return;
            }

            var recs = Ext.getCmp('vpgridratingcurve').store.getRange();
            var data = [];

            for (var c = 0; c < recs.length; c++){
                var rec = recs[c];
                data.push({
                    "from": Ext.Date.format(rec.data.from,'c'),
                    "to": Ext.Date.format(rec.data.to,'c'),
                    "up_val": ""+rec.get("up_val"),
                    "low_val": ""+rec.get("low_val"),
                    "A": ""+rec.get("A"),
                    "B": ""+rec.get("B"),
                    "C": ""+rec.get("C"),
                    "K": ""+rec.get("K")
                });
            }

            Ext.Ajax.request({
                url: Ext.String.format('{0}/istsos/services/{1}/virtualprocedures/{2}/ratingcurve',
                    wa.url,
                    this.istService,
                    Ext.getCmp('vpcmbplist').getValue()),
                scope: this,
                method: "POST",
                jsonData: data,
                success: function(response){
                    var json = Ext.decode(response.responseText);
                    if (!json.success && !Ext.isEmpty(json.message)) {
                        Ext.Msg.alert('Warning', json['message']);
                    }else{
                        this.loadRatingCurve(Ext.getCmp('vpcmbplist').getValue());
                    }
                }
            });

        },this);

        Ext.getCmp('vpbtndeleterc').on('click',function(){

            Ext.Msg.show({
                title:'Erasing rating curve',
                msg: 'Are you sure you want to erase the rating curve data?',
                buttons: Ext.Msg.YESNO,
                icon: Ext.Msg.QUESTION,
                scope: this,
                fn: function(btn){
                    if (btn == 'yes'){
                        Ext.Ajax.request({
                            url: Ext.String.format('{0}/istsos/services/{1}/virtualprocedures/{2}/ratingcurve',
                                wa.url, this.istService, Ext.getCmp('vpcmbplist').getValue()),
                            scope: this,
                            method: "DELETE",
                            success: function(response){
                                var json = Ext.decode(response.responseText);
                                if (!json.success && !Ext.isEmpty(json.message)) {
                                    Ext.Msg.alert('Warning', json['message']);
                                }else{
                                    this.loadRatingCurve(Ext.getCmp('vpcmbplist').getValue());
                                }
                            }
                        });
                    }
                }
            });
        },this);

    },
    loadRatingCurve: function(procedure){
        Ext.getCmp('vppanel').mask.show();
        Ext.getCmp('vpgridratingcurve').store.removeAll();
        Ext.Ajax.request({
            url: Ext.String.format('{0}/istsos/services/{1}/virtualprocedures/{2}/ratingcurve',
                wa.url, this.istService, procedure),
            scope: this,
            method: "GET",
            success: function(response){
                var json = Ext.decode(response.responseText);
                if (json.success) {
                    Ext.getCmp('vpgridratingcurve').store.loadData(json.data);
                }else{
                    console.log(json.message);
                }
                Ext.getCmp('vppanel').mask.hide();
            }
        });
    },
    // Validate single row of the rating curve grid
    validateRatingCurveRecord: function(rec){
        Ext.Array.each(['from','to'], function(key) {
            if (!Ext.isDate(rec.get(key))){
                throw "\""+key+"\" field must be a valid date";
            }
        });
        Ext.Array.each(['A','B','C','K','up_val','low_val'], function(key) {
            if (!Ext.isNumeric(rec.get(key))){
                throw "\""+key+"\" field must be numeric";
            }
        });
    },
    // Validate all rows of the rating curve grid
    validateRatingCurve: function(){
        var recs = Ext.getCmp('vpgridratingcurve').store.getRange();
        if (recs.length==0){
            throw "The rating curve data grid is empty";
        }
        var from, to;
        for (var c = 0; c < recs.length; c++){

            var rec = recs[c];

            try{
                this.validateRatingCurveRecord(rec);
            }catch (e){
                throw 'Line ' + (c+1) + ': ' + e;
            }

            // Check dates
            if (c===0){
                from = rec.get('from');
                to = rec.get('to');
                low = rec.get('low_val');
                up = rec.get('up_val');
            }else{
                if (rec.get('from').getTime() == from.getTime() && rec.get('to').getTime() == to.getTime()){
                    if (rec.get('low_val') != up){
                        throw 'Line ' + (c+1) + ' [Low/Up error]: \"Low\" must be equal to \"Up\" in previous line';
                    }
                }else if (rec.get('from').getTime()  != to.getTime() ){
                    throw 'Line ' + (c+1) + ' [Date error]: \"From\" must be equal to \"To\" in previous line';
                }
                from = rec.get('from');
                to = rec.get('to');
                low = rec.get('low_val');
                up = rec.get('up_val');
            }
            if (from>=to){
                throw 'Line ' + (c+1) + ' [Date error]: \"From\" must be prior to \"To\"';
            }
        }
    }
});