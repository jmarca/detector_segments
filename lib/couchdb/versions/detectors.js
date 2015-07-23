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

// saved as view _design/truckimputed/_view/mints_maxts
var detector_minmax = function(doc) {
    var year_regex = new RegExp("^\\d{4}$");
    var keys = Object.keys(doc);
    var ys = keys.filter(function(k){return year_regex.test(k)});
    var freeway
    var direction
    if( keys.indexOf('properties') === -1 ){
        // find from the year
        ys.some(function(v){
            if(doc[v].properties !== undefined){
                var anyprop = Object.keys(doc[v].properties)[0]
                freeway = doc[v].properties[anyprop].freeway
                direction = doc[v].properties[anyprop].direction
                return true
            }
            return false

        })
    }else{
        // there is a properties file in the main thingee

        var anyprop = Object.keys(doc.properties)[0]
        freeway = doc.properties[anyprop].freeway
        var re = new RegExp("wim\\.\\d+\\.(N|S|E|W)")
        var res = re.exec(doc._id)
        direction = res[1]
    }
    //if(freeway === undefined || direction === undefined){
        // emit([0,doc._id,freeway,direction],null)
        // return null
    //}
    ys.forEach(function(y){
        if(doc[y].mints !== undefined &&
           doc[y].maxts !== undefined){
            emit([+y,freeway,direction,doc[y].mints,doc[y].maxts],null)
        }
        return null
    })
    return null
}
var wimdoc =
        {
            "_id": "wim.10.N",
            "_rev": "432-0f6e06f40d737a4f4a51b2d91918475e",
            "2007": {
                "imputed": "finished",
                "paired": [
                    600060,
                    600060,
                    600060,
                    600280
                ],
                "csv_parse_2cdb": {
                    "d0bb1d83ef67e9e7377ab87f2beff857": {
                        "records": 26280,
                        "file": "./imputed/wim.10.N.truck.imputed.2007.csv"
                    }
                },
                "extract_to_csv": "finished",
                "max_iterations": 0,
                "badpair": 600060,
                "merged": [
                    600060,
                    600060,
                    600060,
                    600280
                ],
                "chain_lengths": [
                    113,
                    105,
                    109,
                    139,
                    110
                ]
            },
            "2008": {
                "imputed": "finished",
                "csv_parse_2cdb": {
                    "160615584f9c947f780382f270d6a0b4": {
                        "records": 26352,
                        "file": "./imputed/wim.10.N.truck.imputed.2008.csv"
                    }
                },
                "paired": [
                    600060,
                    600060,
                    600060,
                    600280
                ],
                "extract_to_csv": "finished",
                "max_iterations": 0,
                "badpair": 600060,
                "merged": [
                    600060,
                    600060,
                    600060,
                    600280
                ],
                "chain_lengths": [
                    8,
                    8,
                    7,
                    8,
                    8
                ]
            },
            "2009": {
                "imputed": "finished",
                "csv_parse_2cdb": {
                    "0e21b883a3dad8a44719c4c131a513c8": {
                        "records": 26280,
                        "file": "./imputed/wim.10.N.truck.imputed.2009.csv"
                    }
                },
                "paired": [
                    600060,
                    600060,
                    600060,
                    600280
                ],
                "extract_to_csv": "finished",
                "max_iterations": 0,
                "merged": [
                    600060,
                    600060,
                    600060,
                    600280
                ],
                "chain_lengths": [
                    6,
                    6,
                    6,
                    7,
                    7
                ],
                "badpair": 600280
            },
            "2010": {
                "imputed": "finished",
                "paired": [
                    600060,
                    600060,
                    600060,
                    600280,
                    639960,
                    629960
                ],
                "merged": 629960
            },
            "2011": {
                "imputed": "finished"
            },
            "2012": {
                "imputed": "finished",
                "merged": "629964",
                "extract_to_csv": "finished",
                "mints": "2012-01-01 00:00:00",
                "maxts": "2012-12-31 23:00:00"
            },
            "properties": {
                "2009-02-25": {
                    "loc": "FRESNO",
                    "wim_type": "Data",
                    "cal_pm": "25",
                    "cal_pm_numeric": 25,
                    "latitude": 36.779,
                    "longitude": -119.85,
                    "freeway": 99,
                    "geojson": {
                        "type": "Point",
                        "crs": {
                            "type": "name",
                            "properties": {
                                "name": "EPSG:4326"
                            }
                        },
                        "coordinates": [
                                -119.85,
                            36.779
                        ]
                    },
                    "lanes": 3,
                    "county": "019",
                    "multiline": true,
                    "abs_pm": 137.3
                }
            }
        }

var doc =
        {
            "_id": "1010510",
            "_rev": "213-a79c7fc62a9a12432d25a428ee494b43",
            "2006": {
                "properties": [
                    {
                        "name": "Obyrnes Ferry Rd",
                        "cal_pm": "11.7",
                        "abs_pm": 51.197,
                        "latitude_4269": "37.888619",
                        "longitude_4269": "-120.494438",
                        "lanes": 1,
                        "segment_length": "5",
                        "freeway": 120,
                        "direction": "W",
                        "vdstype": "ML",
                        "district": 10,
                        "versions": "2006-12-27",
                        "geojson": {
                            "type": "Point",
                            "crs": {
                                "type": "name",
                                "properties": {
                                    "name": "EPSG:4326"
                                }
                            },
                            "coordinates": [
                                    -120.49,
                                37.889
                            ]
                        }
                    }
                ]
            },
            "2007": {
                "vdsdata": "0",
                "occupancy_averaged": 1,
                "vdsraw_chain_lengths": [
                    7,
                    7,
                    7,
                    7,
                    7
                ],
                "wim_neigbors": {
                    "direction": "west",
                    "distance": 6397.5,
                    "wim_id": 99
                },
                "paired_wim": "unmet",
                "vdsimputed": 1,
                "properties": [
                    {
                        "name": "Obyrnes Ferry Rd",
                        "cal_pm": "11.7",
                        "abs_pm": 51.197,
                        "latitude_4269": "37.888619",
                        "longitude_4269": "-120.494438",
                        "lanes": 1,
                        "segment_length": "5",
                        "freeway": 120,
                        "direction": "W",
                        "vdstype": "ML",
                        "district": 10,
                        "versions": [
                            "2007-01-11",
                            "2007-01-31",
                            "2007-02-17",
                            "2007-06-16",
                            "2007-06-21",
                            "2007-06-27",
                            "2007-07-03",
                            "2007-07-12",
                            "2007-07-18",
                            "2007-07-19",
                            "2007-07-31",
                            "2007-08-03",
                            "2007-08-09",
                            "2007-08-11",
                            "2007-08-16",
                            "2007-08-17",
                            "2007-08-30",
                            "2007-09-13",
                            "2007-09-20",
                            "2007-09-22",
                            "2007-09-27",
                            "2007-10-04",
                            "2007-10-06",
                            "2007-10-12",
                            "2007-11-09",
                            "2007-12-14"
                        ],
                        "geojson": {
                            "type": "Point",
                            "crs": {
                                "type": "name",
                                "properties": {
                                    "name": "EPSG:4326"
                                }
                            },
                            "coordinates": [
                                    -120.49,
                                37.889
                            ]
                        }
                    }
                ],
                "truckimputation_max_iterations": 0,
                "csv_parse_2cdb": {
                    "ff3d1d70c50a278f1c8ff55c6225129b": {
                        "records": 41830,
                        "file": "./imputed/vds_id.1010510.truck.imputed.2007.csv"
                    }
                },
                "rawdata": "1",
                "row": 1,
                "truckimputation_chain_lengths": [
                    9,
                    11,
                    9,
                    10,
                    10
                ],
                "truckimputed": "2012-05-10 finish",
                "vdsraw_max_iterations": 0,
                "have_raw_data": "1"
            },
            "2008": {
                "vdsdata": "0",
                "occupancy_averaged": 1,
                "vdsraw_chain_lengths": [
                    10,
                    10,
                    10,
                    10,
                    10
                ],
                "wim_neigbors": {
                    "direction": "west",
                    "distance": 6397.5,
                    "wim_id": 99
                },
                "paired_wim": "unmet",
                "vdsimputed": 1,
                "properties": [
                    {
                        "name": "Obyrnes Ferry Rd",
                        "cal_pm": "11.7",
                        "abs_pm": 51.197,
                        "latitude_4269": "37.888619",
                        "longitude_4269": "-120.494438",
                        "lanes": 1,
                        "segment_length": "5",
                        "freeway": 120,
                        "direction": "W",
                        "vdstype": "ML",
                        "district": 10,
                        "versions": [
                            "2008-01-15",
                            "2008-02-07",
                            "2008-02-09",
                            "2008-03-04",
                            "2008-03-12",
                            "2008-04-19",
                            "2008-06-10",
                            "2008-06-14"
                        ],
                        "geojson": {
                            "type": "Point",
                            "crs": {
                                "type": "name",
                                "properties": {
                                    "name": "EPSG:4326"
                                }
                            },
                            "coordinates": [
                                    -120.49,
                                37.889
                            ]
                        }
                    }
                ],
                "truckimputation_max_iterations": 0,
                "csv_parse_2cdb": {
                    "4eb4c8b8fcf35bc46503e2c2f262d88d": {
                        "records": 43920,
                        "file": "./imputed/vds_id.1010510.truck.imputed.2008.csv"
                    }
                },
                "rawdata": "1",
                "row": 1,
                "truckimputation_chain_lengths": [
                    10,
                    10,
                    10,
                    10,
                    10
                ],
                "truckimputed": "2012-05-10 finish",
                "vdsraw_max_iterations": 0,
                "have_raw_data": "1"
            },
            "2009": {
                "vdsdata": "0",
                "occupancy_averaged": 1,
                "vdsraw_chain_lengths": [
                    9,
                    853,
                    723,
                    302,
                    954
                ],
                "wim_neigbors": {
                    "direction": "west",
                    "distance": 6397.5,
                    "wim_id": 99
                },
                "paired_wim": "unmet",
                "vdsimputed": 1,
                "properties": [
                    {
                        "name": "Obyrnes Ferry Rd",
                        "cal_pm": "11.7",
                        "abs_pm": 51.197,
                        "latitude_4269": "37.888619",
                        "longitude_4269": "-120.494438",
                        "lanes": 1,
                        "segment_length": "5",
                        "freeway": 120,
                        "direction": "W",
                        "vdstype": "ML",
                        "district": 10,
                        "versions": [
                            "2009-01-10",
                            "2009-02-26",
                            "2009-03-25",
                            "2009-03-27",
                            "2009-04-08",
                            "2009-04-09",
                            "2009-04-15",
                            "2009-04-21",
                            "2009-04-22",
                            "2009-05-06",
                            "2009-05-08",
                            "2009-05-16",
                            "2009-05-20",
                            "2009-05-23",
                            "2009-05-30",
                            "2009-06-02",
                            "2009-06-04",
                            "2009-06-19",
                            "2009-07-17",
                            "2009-08-19",
                            "2009-09-11",
                            "2009-09-17",
                            "2009-09-18",
                            "2009-10-24",
                            "2009-11-17",
                            "2009-12-04"
                        ],
                        "geojson": {
                            "type": "Point",
                            "crs": {
                                "type": "name",
                                "properties": {
                                    "name": "EPSG:4326"
                                }
                            },
                            "coordinates": [
                                    -120.49,
                                37.889
                            ]
                        }
                    }
                ],
                "truckimputation_max_iterations": 0,
                "rawdata": "1",
                "csv_parse_2cdb": {
                    "f1a33164e0b765257dbf917df79f9c4b": {
                        "records": 43800,
                        "file": "./imputed/vds_id.1010510.truck.imputed.2009.csv"
                    }
                },
                "row": 1,
                "truckimputation_chain_lengths": [
                    10,
                    10,
                    10,
                    10,
                    10
                ],
                "truckimputed": "2012-05-10 finish",
                "vdsraw_max_iterations": 0,
                "have_raw_data": "1"
            },
            "2010": {
                "properties": [
                    {
                        "name": "Obyrnes Ferry Rd",
                        "cal_pm": "11.7",
                        "abs_pm": 51.197,
                        "latitude_4269": "37.888619",
                        "longitude_4269": "-120.494438",
                        "lanes": 1,
                        "segment_length": "5",
                        "freeway": 120,
                        "direction": "W",
                        "vdstype": "ML",
                        "district": 10,
                        "versions": [
                            "2010-06-24",
                            "2010-08-27"
                        ],
                        "geojson": {
                            "type": "Point",
                            "crs": {
                                "type": "name",
                                "properties": {
                                    "name": "EPSG:4326"
                                }
                            },
                            "coordinates": [
                                    -120.49,
                                37.889
                            ]
                        }
                    }
                ],
                "rawdata": "do not have counts in left lane in raw vds file",
                "vdsraw_chain_lengths": "raw data failed basic sanity checks",
                "occupancy_averaged": 1
            },
            "2012": {
                "properties": [
                    {
                        "name": "Obyrnes Ferry Rd",
                        "cal_pm": "11.7",
                        "abs_pm": 51.197,
                        "latitude_4269": "37.888619",
                        "longitude_4269": "-120.494438",
                        "lanes": 1,
                        "segment_length": "5",
                        "freeway": 120,
                        "direction": "W",
                        "vdstype": "ML",
                        "district": 10,
                        "versions": [
                            "2012-09-25",
                            "2012-10-04",
                            "2012-11-15",
                            "2012-12-14"
                        ],
                        "geojson": {
                            "type": "Point",
                            "crs": {
                                "type": "name",
                                "properties": {
                                    "name": "EPSG:4326"
                                }
                            },
                            "coordinates": [
                                    -120.49,
                                37.889
                            ]
                        }
                    }
                ],
                "vdsraw_chain_lengths": [
                    5,
                    5,
                    5,
                    5,
                    5
                ],
                "vdsraw_max_iterations": 0,
                "occupancy_averaged": 1,
                "truckimputation_chain_lengths": [
                    7,
                    8,
                    8,
                    7,
                    8
                ],
                "truckimputation_max_iterations": 0,
                "mints": "2012-01-01 00:00:00",
                "maxts": "2012-12-31 23:00:00"
            },
            "2013": {
                "properties": [
                    {
                        "name": "Obyrnes Ferry Rd",
                        "cal_pm": "11.7",
                        "abs_pm": 51.197,
                        "latitude_4269": "37.888619",
                        "longitude_4269": "-120.494438",
                        "lanes": 1,
                        "segment_length": "5",
                        "freeway": 120,
                        "direction": "W",
                        "vdstype": "ML",
                        "district": 10,
                        "versions": [
                            "2013-01-09",
                            "2013-01-23",
                            "2013-01-24",
                            "2013-03-08",
                            "2013-03-12",
                            "2013-04-05",
                            "2013-04-30",
                            "2013-07-02",
                            "2013-07-03",
                            "2013-07-13",
                            "2013-08-09",
                            "2013-10-01",
                            "2013-10-03",
                            "2013-10-09",
                            "2013-10-10",
                            "2013-10-12",
                            "2013-10-15",
                            "2013-11-07",
                            "2013-11-09",
                            "2013-11-13"
                        ],
                        "geojson": {
                            "type": "Point",
                            "crs": {
                                "type": "name",
                                "properties": {
                                    "name": "EPSG:4326"
                                }
                            },
                            "coordinates": [
                                    -120.49,
                                37.889
                            ]
                        }
                    }
                ]
            },
            "2014": {
                "properties": [
                    {
                        "name": "Obyrnes Ferry Rd",
                        "cal_pm": "11.7",
                        "abs_pm": 51.197,
                        "latitude_4269": "37.888619",
                        "longitude_4269": "-120.494438",
                        "lanes": 1,
                        "segment_length": "4.225",
                        "freeway": 120,
                        "direction": "W",
                        "vdstype": "ML",
                        "district": 10,
                        "versions": [
                            "2014-01-25",
                            "2014-01-31",
                            "2014-02-05",
                            "2014-02-22"
                        ],
                        "geojson": {
                            "type": "Point",
                            "crs": {
                                "type": "name",
                                "properties": {
                                    "name": "EPSG:4326"
                                }
                            },
                            "coordinates": [
                                    -120.49,
                                37.889
                            ]
                        }
                    }
                ]
            }
        }


detector_minmax(doc)

detector_minmax(wimdoc)
