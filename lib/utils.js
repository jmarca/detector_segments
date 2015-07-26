var direction_lookup ={'N':'north'
                      ,'S':'south'
                      ,'W':'west'
                      ,'E':'east'
                      };
function convertDetectorIdForSQLwhere(did){
    did = String(did)
    var numericpart = did.match(/\d+/);
    // assume vds, then test for wim
    var detector_id = numericpart[0];
    var whereclause = ['detector_id=vdsid_'+detector_id];
    // special handling for WIM
    if(/wim/.test(did)){
        // WIM data has a direction
        var directionpart = did.match(/\.(.)$/);
        var dir = direction_lookup[directionpart[1]];
        whereclause = ['detector_id=wimid_'+detector_id
                      ,'direction='+dir
                      ];
    }
    return whereclause;
}
function convertDetectorIdForSQL(did){
    did = String(did)
    var numericpart = did.match(/\d+/);
    // assume vds, then test for wim
    var detector_id = numericpart[0];
    var sqlid = 'vdsid_'+detector_id;
    // special handling for WIM
    if(/wim/.test(did)){
        // WIM data has a direction
        var directionpart = did.match(/\.(.)$/);
        var dir = direction_lookup[directionpart[1]];
        sqlid = 'wimid_'+detector_id
    }
    return sqlid;
}
exports.direction_lookup = function(d){ return direction_lookup[d] }
exports.convertDetectorIdForSQL=convertDetectorIdForSQL
exports.convertDetectorIdForSQLwhere=convertDetectorIdForSQLwhere
