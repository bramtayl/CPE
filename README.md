This repository contains the code I used to migrate the data for the Center for
Popular Economics (CPE) to CiviCRM.

The old data was scattered across several sources: an Microsoft Access database,
mailing lists in Rise Up and MailChimp, and supplemental spreadsheets. I have
not made this data publicly available.

I used R to combine the data, clean it, and put it into a form to match the
CiviCRM database. Finally, I directly uploaded the data to the MySQL backend for
our website server.

The entry point is `CPE.R`, which will call all the other files. If you would
like to delete everything and start again, run `delete.R`.

I used the `mdb.get` function from the `Hmisc` package to interface with Access.
This was less than ideal; I had to create a patched version of it to fix a bug.
A preferred option would have been to use ODBC with Microsoft Access, but I
couldn't do that because I'm running Linux.
