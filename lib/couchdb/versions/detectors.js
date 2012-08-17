function emit(a,b){
    console.log("emit "+JSON.stringify(a)+" " +JSON.stringify(b))
}

var mapme =
function(doc) {

    var wimre = new RegExp("^wim.*(N|S|E|W)");
    var d12re = new RegExp("^12");
    if(  ! wimre.test(doc._id) ){

        var year_regex = new RegExp("^\\d{4}$");
        var keys = Object.keys(doc);
        var ys = keys.filter(function(k){return year_regex.test(k)});
        // search each year, stop at first one with properties with hwy
        var props;

        var findprops =  function(y){
                if(doc[y].properties === undefined){
                    return null;
                }

                doc[y].properties.some(function(p){
                    if(p.freeway === undefined || p.vdstype === undefined || p.vdstype !== 'ML'){
                        return null;
                    }
                    props = p;
                    p.versions.forEach(function(v){
                        emit([v,props.freeway - 0.0,props.direction],[doc._id,props]);
                    })
                    return null;
                });
                return null;
            };
        ys.forEach(findprops);

    } else if(  wimre.test(doc._id) && doc.properties !== undefined ){
        var result = wimre.exec(doc._id)
        var direction = result[1]
        var year_regex = new RegExp("^\\d{4}$");
        var keys = Object.keys(doc);
        var ys = keys.filter(function(k){return year_regex.test(k)});
        // search each year, stop at first one with properties with hwy
        var props = doc.properties;
        var pk = Object.keys(props)
        var pk0 = pk[0]
        var findprops =  function(y){
                // here we need to decide whether to emit a year.
                if(doc[y].max_iterations !== undefined
                   && doc[y].max_iterations == 0){
                    emit([[y,'01','01'].join('-'),props[pk0].freeway - 0.0,direction],[doc._id,props]);
                }
            }
        ys.forEach(findprops)
        return null;
    }
    return null;
};

