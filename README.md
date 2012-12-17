# Detector Segments

So.  I've got detectors that produce data. Also there are gaps in the
data.  I used R (amelia) to impute the missing data, and have stored
that in several CouchDB databases.  Then I also used R to impute other
stuff, also stashed in CouchDB.

This project exists to query the CouchDB databases and pull out the
start and end times of activity for each detector for each year.  Then
with that information I can build a complete network of detectors on
roadways.  This will then let me assign approximate lengths to
detectors, because I know the active upstream and downstream
detectors, and can split the distance to each in order to estimate the
length of roadway being measured by the detector in question.
