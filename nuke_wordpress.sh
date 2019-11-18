mysql --user root --password=YOUR_PASSWORD_HERE < nuke_wordpress.sql
# remove civicrm contents, and create a new writable folder for civicm
rm --recursive --force /usr/share/wordpress/wp-content/uploads/civicrm
mkdir /usr/share/wordpress/wp-content/uploads/civicrm
chmod a+rwx /usr/share/wordpress/wp-content/uploads/civicrm
