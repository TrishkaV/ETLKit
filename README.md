# **ETLKit**
**Lightweight C# language extension for ETL operation with optimal memory allocation, this library provides a series of methods for ETL operations on different data types.**<br><br>

add "**using ETLKit;**" to enable the language extension and call the ETL methods on available all data types.
<br><br>

**Examples:**

**COLLECTIONS<br>**
".ToDictionary()" for list of two item tuples where the first element is the key and the second is the value.<br>
".AsBatches()" turns a list into custom size batches of the same elements to facilitate processing.<br>
".AppendOrReplace()" for dictionaries.<br>
<br><br>
**COLLECTIONS ADVANCED<br>**
".AppendCalc()" appends a dictionary to another, with the option of applying a data transformation formula to it.<br>
".ConvertAll()" apply a data transformation formula to a Dictionary.<br>
".ConvertAsCastable()" apply a data transformation formula to a Dictionary with no regards to data type (returns a dynamic object that can be casted).
<br><br>

**TIME<br>**
".ToDateTime()" and ".ToDateTimeUTC()" for strings.<br>
".ToEpoch()" which returns a unix timestamp and ".ToIso8601()" which creates a ISO8601-compatible string for DateTime.<br>
".ToDateTime()" and "ToDateTimeUTC()" for "long" data type (used to store a unix timestamp).<br>
<br><br>
**From .NET 7<br>**
".In()" is syntactic sugar to check if an object is in a collection of the same type, e.g. --> if ("my_string".In("myString", "MYSTRING", "my_string")) { } would be "true".<br>

<br><br>

NOTE<br>
Legally this comes with no warranty and you are using it at your own risk.
<br><br>
This library have been tested agaist real database extractions and objects of all the mentioned types its results hold correct.
<br><br>
If you find an issue with the results or implementation or an optimization could be made please feel free to contact me or issue a pull request.
You will be credited for any contribution.
