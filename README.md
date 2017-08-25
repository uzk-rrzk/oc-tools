# README #

University of Cologne's Opencast utility scripts, authored mainly by Rubén Pérez Vázquez.

### What is this repository for? ###

This is a miscellaneous repository were I put the scripts I have at some point created to solve a particular problem. I cannot guarantee these will work for anybody else, but I am making them available to the general public, in case someone can find them useful.

### How do I get set up? ###

You never get set up with Opencast. It is like trying to tame a wild animal: they may behave for a while, but you never know when it will snap back at you and chop your head off (metaphorically speaking...)
Think of these scripts as a whip: they may occasionally keep problems at bay, but for a long-term solution, you may have to deal with the beast itself, maybe change a few lines of the beast's code or refactor the beast into a pony, depending on your time/patience/mental health/craftsmanship.

Jokes aside, most of the scripts are Python scripts using the ArgParse library, so you can see a brief parameter description by running:

    python <script_name> -h
	
If that help is not enough, there's no help at all, or you ~~just want to bother me~~ have any other petition, please write to the email below.

### Contribution guidelines ###

None at all. Just contact me and we'll see what we can do.

### Who do I talk to? ###

All this work was done as part of my job at the University of Cologne.
Please report problems, send your praise, criticism, monetary prizes, death threats and so on to: Rubén Pérez <ruben.perez@uni-koeln.de>

### Content summary ###

* **`extract_mp_ids.py`**: Extract full list of Mediapackage IDs present in either the archive of the search index
* **`mh_clean_series.py`**: Delete empty series in an Opencast system
* **`mh_clean_unarchived.py`**: Delete all workflows that belong to mediapackages that are not/no longer archived
* **`mh_clean_workflows.py`**: Delete workflows based on their state
* **`mh_edit_published_urls.py`**: Edit URLs in mediapackages published in Opencast, for instance when a download server URL changes.
* **`mh_export.py`**: Download all the published videos in a Matterhorn series
* **`migration`**: Scripts to perform a migration of mediapackages between Matterhorn/Opencast systems
* **`SelectSeries.py`**: Create a list of series in a file (normally to migrate them using the scripts above)
* **`old`**: Older scripts. They are not guaranteed to work or be relevant anymore (even less so than the others!)