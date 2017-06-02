# Guru Cue Search &amp; Recommendations Data Provider for PostgreSQL

This is the library implementing the database interface for the Guru Cue Search
&amp; Recommendations, using a PostgreSQL database.

# Building the Library
The minimum required JDK version to build the library with is 1.8.

Before you start building the library put into the `libs` directory these Guru
Cue Search &amp; Recommendations libraries:
* `database`,
* `data-provider-jdbc`.

Perform the build using [gradle](https://gradle.org/). If you don't have it
installed, you can use the gradle wrapper script `gradlew` (Linux and similar)
or `gradlew.bat` (Windows).

As part of the build process basic instantiation tests are run which load
and cache metadata, consumers, and products in the caching range (time-wise).
This takes time and memory. For non-production purposes it is thus usually
convenient to skip tests. E.g. using the gradle wrapper this is accomplished
with:
```text
./gradlew -x test
```

The build process will result in a `jar` file in the `build/libs` directory.
Copy it into the `libs` directory of dependent projects, such as `rest-api`.

# Creating the Database
Use the [SQL script](sql/create.sql) to create the database. The script will
also insert basic meta-data, such as languages and attributes, some of which
is hard-coded in the source for performance reasons.
