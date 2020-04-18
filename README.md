# Energy Market Analysis
 
### *Exploring NEM open data from Australian energy markets*

The [NEM website](http://nemweb.com.au) contains a wealth of data about the Australian energy market and most of it is updated on a frequent basis. Before even proceeding to basic EDA, setting up a dynamic data extraction pipeline is quite necessary. Manually downloading and unzipping 1000s of files is probably not a great use of time.
<br></br>

## Data Extraction Tools

So far, I have created a couple of tools to extract relevant data to explore the determinants of energy demand:

### NEM-TEL: National Energy Market - Track, Extract Load

This tool can track user-specified parts of the [NEM website](http://nemweb.com.au) to identify any new data files that have been added and not yet downloaded - the *track* part. The *extract* part takes care of downloading the data files in an automated fashion, while the *load* part is used to load the data files into an (almost) analysis-ready format. The `nemtel_demo.ipynb` notebook provides more details.<sup>[A](#footnoteA)</sup> 

### BOM-Data: Bureau of Meteorology Data

This tool has the ability to scrape the latest, as well as historical publicly-available weather data from selected weather stations in Australia. The `bomdata_demo.ipynb` notebook shows how this simple tool can be used. 
<br></br>

## Exploratory Data Analysis

The `demand_analysis.ipynb` notebook contains some preliminary visual analysis of electricity demand in NSW using both NEM and BOM data. 

<br>

*More to come...*

<br>

***
<sub>
<a name="footnoteA">A</a>: Note there is already a Python package for importing NEM data into local SQLite databases called <a href="https://github.com/opennem/nemweb">opennem</a>. My work is quite preliminary and tailored to my needs, so I would suggest this as an alternative for Pythonistically accessing NEM data. 
</sub>
