# Energy Market Analysis
 
### *Exploring NEM open data from Australian energy markets*

The [NEM website](http://nemweb.com.au) contains a wealth of data about the Australian energy market and most of it is updated on a frequent basis. Before even proceeding to basic EDA, setting up a dynamic data extraction pipeline is quite necessary. Manually downloading and unzipping 1000s of files is probably not a great use of time.

So far, I have created what I call a *track, extract, load* pipeline dubbed NEM-TEL. This preliminary tool can track user-specified parts of the [NEM website](http://nemweb.com.au) to identify any new data files that have been added and not yet downloaded - the *track* part. The *extract* part takes care of downloading the data files in an automated fashion, while the *load* part is used to load the data files into an (almost) analysis-ready format. The `nemtel_demo.ipynb` notebook provides more details. 

Note there is already a Python package for importing NEM data into local SQLite databases called [opennem](https://github.com/opennem/nemweb). What I've developed is a bit more tailored to my needs, but I just wanted to get it out in the open source in case anyone finds something useful inside.  