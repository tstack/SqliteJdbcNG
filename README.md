
[![Build Status](https://travis-ci.org/tstack/SqliteJdbcNG.png)](https://travis-ci.org/tstack/SqliteJdbcNG)

SqliteJdbcNG
============

SqliteJdbcNG is a new JDBC driver for SQLite.  The overall goal of this project is to start a
fresh implementation that leverages newly available technologies in the Java world.  For example,
any SQLite driver for any language must integrate with the native SQLite library.  All of the
current Java implementations rely on a custom JNI library to call out to the SQLite library.
This extra layer can easily create a headache for the development and deployment of the driver
since it needs to be built for a variety of operating systems.  Fortunately, there are technologies
like [Bridj](http://code.google.com/p/bridj/) and [JNA](https://github.com/twall/jna) that can
be used to call native code directly from Java.  By leaving the majority of the headaches of
integrating with the native library to the Bridj project, more time can be spent on making a high
quality driver that is more compliant with the JDBC spec.


*NOTE*: This project is still in its early stages and, while functional right now, it is not ready
for real use.

Priorities
----------

The following are the main goals and priorities for this project:

1. Avoid SQLite-specific JNI libraries.  Using the Bridj library means that we do not have to worry
 about setting up build scripts and writing stubs by hand, which can be error-prone.
1. Compliance with the [JDBC spec](http://download.oracle.com/otndocs/jcp/jdbc-4_1-mrel-spec/index.html)
 -- SQLite is a well-engineered piece of software and deserves a JDBC driver of similar quality.
1. Performance -- The Bridj library claims to be close to JNI in performance, so we should be able
 to produce an implementation that is competitive.
1. Customizability -- SQLite provides a means to define custom functions and collators that can be
 used from SQL queries.  These features are nice, however, the above goals should always come first.



Existing Drivers
----------------

* <http://www.ch-werner.de/javasqlite/>
* <https://bitbucket.org/xerial/sqlite-jdbc>
