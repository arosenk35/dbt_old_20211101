Challenges on Patient:
1:patient only male/female no Other/blank .... online we can use Other
2:patient address2 can't be null .... weird but we can work around that!
3: unable to associate charge account to a patient  seems the charge flag option is missing (major issue!!!!)

Payload challenges on GET:
1: payload can the rows be in a different json element e.g. "ips_data" versus "Data" is causing a little bit of confusion in the IPASS platforms as they return all thier resukt as element "data" .... nice to have fix 
2: Payload loads that are returned are they returned as pure json? still tying to figure out what is goping on with the ips payloads i am required to parse it in our ipaas platform .... which is weird as i have intergrated with 20+ diff apps/apis at this is a first.

3: FIELDS ARE LOWER/CAPS NOT CONSISTENT   ...... luxury

4: WHERE WITH MULITPLE LIKES fails -1000 .... e.g. PATIENTFIRSTNAME like %LINDA% AND PATIENTLASTNAME like %test%
MIXED CASE LIKE?   ..... (issue)


