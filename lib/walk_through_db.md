# This is good, but out of date

The following talks about the pre-2015 way of figuring out start stop
times by walking through the list of detectors on CouchDB.  It is
accurate and a decent write-up, but probably not relevant to the
actual code here anymore

# walking through couchdb by detector

I've been using the raw dbs, but now I think I have figured out how to
use the collated dbs, and to walk through the detector list without
having to resort to view building.

First, to get the first doc, just query the first doc in the db

``` bash
james@kitty ~$ curl "127.0.0.1:5984/imputed%2Fc2%2Fd12%2F2007/_all_docs?limit=1"
{"total_rows":4254096,"offset":0,"rows":[
{"id":"1201054-2007-01-01 00:00","key":"1201054-2007-01-01 00:00","value":{"rev":"1-3443ea3707600e3ae9e4cd69de48ea40"}}
]}
```

That provides both the first detector, and its starting time.  Then
the ending time is the following query:

``` bash
james@kitty ~$ curl "127.0.0.1:5984/imputed%2Fc2%2Fd12%2F2007/_all_docs?limit=1&startkey=%221201054-2008%22&descending=true"
{"total_rows":4254096,"offset":4249046,"rows":[
{"id":"1201054-2007-07-30 09:00","key":"1201054-2007-07-30 09:00","value":{"rev":"1-b56c456c67ad78e740e8c5d728780b9f"}}
]}
```

That is, given the year is 2007, I know that all dates must be less
than this.  So the guaranteed last+1 id is the detector id, plus the
year 2008 (year + 1).  Adding the descending=true option will put the
*end* at the start of the returned batch, thus giving the very last
document timestamp for this detector.

The next detector and start time is found by the following query:

``` bash
james@kitty ~$ curl "127.0.0.1:5984/imputed%2Fc2%2Fd12%2F2007/_all_docs?limit=1&startkey=%221201054-2008%22&descending=false"
{"total_rows":4254096,"offset":5050,"rows":[
{"id":"1201066-2007-01-01 00:00","key":"1201066-2007-01-01 00:00","value":{"rev":"1-649c6f229f4e5b3712dab867de5206b0"}}
]}
```

That is, exactly the same as the previous query, but with
descending=false.

Thus there is an iterative procedure, and as long as the responses are
reasonable, you can walk your way through every detector in the
database one query at a time.  Given that there are less than a
thousand or so detectors ever in a district, that is okay.

The stopping condition: when the response has no rows.
