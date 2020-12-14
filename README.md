# InfluxDB Delete Tool (IDT)
A inofficial tool to clean up points with bad values. It allows to query with all supported WHERE clause including by tag **and** field values.

### The problem
InfluxDB provides very limited delete functionality. Out of the the box it can only DROP full databases or measurements. Otherwise it has the [DELETE](https://docs.influxdata.com/influxdb/v1.8/query_language/manage-database/#delete-series-with-delete), which allows some more control, but only filtering by tags or time.
In most cases there are many points with the same tags, so this is not suitable. I needs a way to filter by fields (actual data) values to clean up just the bad values, like requested in [this 5 year old issue](https://github.com/influxdata/influxdb/issues/3210). This is exactly what this tool solves.

### How it works
This tool separates the selection and filtering of points and the actual deletion. This solves the issue because the SELECT supports much more filtering with [WHERE](https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#the-where-clause). First step is therefore to run a SELECT and check if it yields the correct points. After confirmation it runs the deletion using just the exact timestamps.

### Usage
<!-- TODO -->

### Limitation