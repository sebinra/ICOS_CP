# ICOS_CP
prepare and send met files to Carbon portal

This script has been prepared by Christophe Chippeau for the Salles Bilos ICOS site.
It has been generalized by Sébastien Lafont.

The input files are : 

* a datalogger file 
* a header file validated by the ETC
* a correspondance file that link each ETC variable name with the datalogger variable name

The script :
* read the datalogger file
* keep only the last days of the file (to save some computation)
* create a continuous dataset (no time gap) with a timestamp format compliant with ETC
* rename and order the columns according to the ETC header file
* generate daily files (one file per 24 hour period)
* send the last day of the file to the carbon portal
* keep a copy of the file sent to the carbon portal


This script run with Python (and eventually a bash script).

