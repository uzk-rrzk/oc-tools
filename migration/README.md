# University of Cologne's Opencast Migration Scripts #

*This is a preliminary version of the documentation. Improvement suggestions (or, even better, pull requests) are welcome*

**DISCLAIMER** I do not take responsibility if those scripts cause data loss in your system or any other problem. They were designed to work correctly (and they do) in the University of Cologne's environment, and they should not do anything harmful in your system, but depending on the configuration you provide, they may. So make sure you do a test 

This documentation attempts to describe how to migrate contents between an Opencast installation (source) to other (destination), possibly running a different version of the software.

The scripts must run in a `admin` or `ingest` server of the destination cluster. The server must have mounted the NFS share of the destination systems. This is necessary because there are resource URLs (namely: the streaming URLs) which cannot be remotely download. Instead, the script attempts to "translate" the URLs in the mediapackage into filesystem paths and reconstructs the mediapackage with those. Then, it zips the mediapackage and ingests it using the "inbox" facilities of the destination system.

It must be in an `ingest` or `admin` server because the "fileinstaller" class used by Opencast to detect when a new file is copied in the inbox does not work on remote machines --the files must be copied from the same machine or they won't be detected. If you know a way to overcome this limitation, please share it with me so that I can document it here.

The destination system must define an inbox where the migrated mediapackages will be copied. There's a sample inbox configuration file in the `oc-config` folder, as well as a sample workflow to migrate the mediapackages. You may use them verbatim (although you may want to adapt the paths) or modify them to your liking.

The script assumes that all the relevant mediapackages are archived and optionally published. I haven't tested whether the scripts work with published mediapackages that have not been archived. If it does not, it should not be hard to fix, though.

Before starting, please read the contents in `config.py` and adjust the values there to your scenario. You may definitely want to fill in the `scr_user`, `src_pass`, `src_admin` and `src_engage` parameters, and their counterparts starting with `dst_`. Those are the **DIGEST** credentials to the servers and their URL addresses. Some endpoints (namely, the archive endpoint, which was somewhere between 1.x and 2.x) may require being changed. In any case, if you don't understand a parameter, or you don't know how to adjust some parameter to match your particular scenario, please email me.

Once the `config.py` has been adjusted, you must generate a list of series to migrate and put them in a file the scripts can access to. It must be one series id per line. Comments starting with `#` are supported, as well as annotations immediately after the series ID. In other words, the lines *must* start with a series ID followed by a white space. Whatever comes after that is ignored. 

In order to generate this file, the `SelectSeries.py` is available. **This script must not run on a server, but in your desktop computer**. It will help you see all the series in your system and generate a file with a list of series according to the syntax defined above. 

These is no support for partial ingestions of a series. There's no support for ingesting MPs without series. Both things should not be hard to implement, though.

Once the script has a list of series to ingest, when run, it will start at the first one and migrate all the mediapackage found in the series sequentially. When it finished, it will do the same with the second one, etc, until it finished the series list. In order to throttle the whole migration process, a `-i` tag ("do-not-iterate") can be used. That will migrate the next mediapackage in the sequence, if any. Should an error occur, the script will go on until it migrates a MP without errors, or reaches the end of the list. A cronjob can be configured to run the script at regular intervals (there's an example in the `oc-config` directory).

The script does not support concurrency. If the cronjob is set to a very short interval, it could cause two script runs to overlap. To avoid this, a very simple "pidfile" mechanism was implemented. It should work for a normal using of the script, but it may fail if two instances of the script are started almost simultaneously (less than a second of difference).

Migrated mediapackages are marked with a hidden file `.ingested`, in the temporary folder this script uses. Series are marked in a similar way, when all the mediapackages in the series are succesfully migrated. Similarly, failed mediapackages (i.e. mediapackages that could not be successfully migrated) and series with failed mediapackages are marked with a hidden file `.failed`. Mediapackages and series that are marked with any of those files are NOT checked again. If you want to retry the migration of an already-failed mediapackage, you must delete the file and run the script again.