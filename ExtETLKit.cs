using System.Globalization;

namespace ETLKit;

/// <summary>
/// <para>Lightweight C# language extension for ETL operation, this library provides a series of methods for ETL operations on different data types in a performance optimized way.</para>
/// <para>Examples:</para>
/// <para>COLLECTIONS</para>
/// <para>".ToDictionary()" for list of two item tuples where the first element is the key and the second is the value.</para>
/// <para>".AsBatches()" turns a list into custom size batches of the same elements to facilitate processing.</para>
/// <para>".AppendOrReplace()" for dictionaries.</para>
/// <para>-- Collections advanced</para>
/// <para>".AppendCalc()" appends a dictionary to another, with the option of applying a data transformation formula to it.</para>
/// <para>".ConvertAll()" apply a data transformation formula to a Dictionary.</para>
/// <para>".ConvertAsCastable()" apply a data transformation formula to a Dictionary with no regards to data type (returns a dynamic object that can be casted).</para>
/// <para>TIME</para>
/// <para>".ToDateTime()" and ".ToDateTimeUTC()" for strings.</para>
/// <para>".ToEpoch()" which returns a unix timestamp and ".ToIso8601()" which creates a ISO8601-compatible string for DateTime.</para>
/// <para>".ToDateTime()" and "ToDateTimeUTC()" for "long" data type (used to store a unix timestamp).</para>
/// <para>From .NET 7</para>
/// <para>".In()" is syntactic sugar to check if an object is in a collection of the same type, e.g. --> if ("my_string".In("myString", "MYSTRING", "my_string")) { } would be "true".</para>
/// </summary>
public static class ExtETLKit
{
    #region IEnumerable
    /// <summary>
    /// Zips 2 IEnumerables with the same lenght into a Dicionary.
    /// </summary>
    public static Dictionary<K, V> ToDictionaryWithValues<K, V>(this IEnumerable<K> keys, IEnumerable<V> values) where K : notnull
    {
        if (keys.Any(key => key == null))
            throw new ArgumentException("Keys cannot be null.");

        if (keys.Count() != values.Count())
            throw new ArgumentException($"The keys list has {keys.Count()} elements, while the values list has {values.Count()}.");

        return keys.Zip(values, (key, value) => new { key, value }).ToDictionary(x => x.key, x => x.value);
    }

    /// <summary>
    /// Turn an IEnumerable of 2 values tuples into a Dictionary.
    /// </summary>
    public static Dictionary<K, V> ToDictionary<K, V>(this IEnumerable<(K, V)> input) where K : notnull => input.ToDictionary(x => x.Item1, x => x.Item2);

    /// <summary>
    /// Split an IEnumerable into variable size chunks.
    /// </summary>
    public static List<IEnumerable<T>> AsBatches<T>(this IEnumerable<T> input, int batchSize = 1_000)
    {
        var batches = new List<IEnumerable<T>>();
        var nElements = input.Count();
        var currentPosition = 0;

        while (currentPosition < nElements)
        {
            batches.Add(input.Skip(currentPosition).Take(batchSize));
            currentPosition += batchSize;
        }

        return batches;
    }
    #endregion IEnumerable

    #region Dictionary
    /// <summary>
    /// All the values of the parameter Dictionary overwrite the same-key values in the caller Dictionary
    /// or are added to it if not present.
    /// </summary>
    public static void AppendOrReplace<K, V>(this Dictionary<K, V> first, in Dictionary<K, V> second) where K : notnull
    {
        foreach (var i in second)
            first[i.Key] = i.Value;
    }

    /// <summary>
    /// <para>
    /// "ReturnOnly" version of "AppendOrReplace()"</para>
    /// <para>
    /// Returns a Dictionary where the values of the parameter Dictionary overwrite the same-key values in the caller Dictionary
    /// or are added if not present. Neither of the dictionaries are altered, just a new one is returned.</para>
    /// </summary>
    public static Dictionary<K, V> AppendOrReplaceRO<K, V>(this Dictionary<K, V> first, in Dictionary<K, V> second) where K : notnull
    {
        var result = new Dictionary<K, V>();
        foreach (var i in first)
            result[i.Key] = i.Value;
        foreach (var i in second)
            result[i.Key] = i.Value;

        return result;
    }

    /// <summary>
    /// Perform the "cumulator" parameter function between the values of the Dictionary passed as parameter onto the caller Dictionary matching on keys,
    /// non-matching keys are added to the caller Dictionary.
    /// <para>
    /// WARNING: only use with Dictionaries that have number types as values (int, long, double...).
    /// No checks are performed: if using different types (e.g. adding long and int) consider using the long
    /// Dictionary as the caller.</para>
    /// <para>
    /// Example:</para>
    /// <para>
    /// var myDic = { "first": 10, "second": 20 }</para>
    /// <para>
    /// var otherDic = { "first": 5, "second": 2, "third": 30 }</para>
    /// <para>
    /// myDic.AppendCalc(otherDic, cumulator: (x, y) =&gt; x * y); </para>
    /// <para>
    /// Result:</para>
    /// <para>
    /// myDic -&gt; { "first": 50, "second": 40, "third": 30 }</para>
    /// <para>
    /// It is optionally possible to pass "targetModifier" and "inputModifier" parameter functions
    /// to alter the Dictionaries before the modifier function is performed. The Dictionary passed
    /// as parameter is never altered.</para>
    /// <para>Example alternative:</para>
    /// <para>
    /// eg: myDic.AppendCalc(otherDic, targetModifier: x => x + 1, inputModifier: x => x * 5, cumulator: (x, y) => x + y);</para>
    /// <para>
    /// Result:</para>
    /// <para>
    /// myDic -&gt; { "first": 36, "second": 31, "third": 150 }</para>
    /// </summary>
    public static void AppendCalc<K, V>(this Dictionary<K, V> target, in Dictionary<K, V> input, Func<dynamic, dynamic, dynamic> cumulator, Func<dynamic, dynamic>? targetModifier = null, Func<dynamic, dynamic>? inputModifier = null) where K : notnull where V : notnull
    {
        targetModifier ??= x => x;
        inputModifier ??= x => x;

#if DEBUG
        foreach (var i in input)
        {
            if (target.ContainsKey(i.Key))
            {
                var modTarget = targetModifier(target[i.Key]);
                var modInput = inputModifier(i.Value);
                target[i.Key] = cumulator(modTarget, modInput);
            }
            else
                target[i.Key] = inputModifier(i.Value);
        }
#else
        foreach (var i in input)
        {
            if (target.ContainsKey(i.Key))
                target[i.Key] = cumulator(targetModifier(target[i.Key]), inputModifier(i.Value));
            else
                target[i.Key] = inputModifier(i.Value);
        }
#endif
    }

    /// <summary>
    /// Applies the modifier function to all Dictionary values.
    /// <para>
    /// Example:</para>
    /// <para>
    /// If the "myDic" Dictionary contains { "first": 10, "second": 20 } </para>
    /// <para>
    /// calling myDic.ConvertAll(x =&gt; x / 5);</para>
    /// <para>
    /// results in:</para>
    /// <para>
    /// { "first": 2, "second": 4 }</para>
    /// </summary>
    public static void ConvertAll<K, V>(this Dictionary<K, V> target, Func<dynamic, dynamic> modifier) where K : notnull where V : notnull
    {
        /* eg: .ConvertAll(x => x / 5); */
        foreach (var t in target)
            target[t.Key] = modifier(t.Value);
    }

    /// <summary>
    /// <para>
    /// "ReturnOnly" version of "ConvertAll()"</para>
    /// <para>
    /// Applies the modifier function to all Dictionary values.</para>
    /// <para>
    /// Example:</para>
    /// <para>
    /// If the "myDic" Dictionary contains { "first": 10, "second": 20 } </para>
    /// <para>
    /// Calling myDic.ConvertAll(x =&gt; x / 5);</para>
    /// <para>
    /// Returns:</para>
    /// <para>
    /// { "first": 2, "second": 4 }</para>
    /// <para>
    /// but does not change the original Dictionary "myDic".</para>
    /// </summary>
    public static Dictionary<K, V> ConvertAllRO<K, V>(this Dictionary<K, V> first, Func<dynamic, dynamic> modifier) where K : notnull where V : notnull
    {
        var result = new Dictionary<K, V>();
        foreach (var i in first)
            result[i.Key] = modifier(i.Value);

        return result;
    }

    /// <summary>
    /// <para>Convert the values of the Dictionary with the modifier function passed and returns a "dynamic" of it that can be casted.</para>
    /// <para>
    /// Example:</para>
    /// <para>
    /// var myDic = new Dictionary&lt;string, int&gt;();</para>
    /// <para>
    /// var myConverted = (Dictionary&lt;string, long&gt;)myDic.ConvertAsCastable(x =&gt; Convert.ToInt64(x));</para>
    /// <para>
    /// Result:</para>
    /// <para>
    /// "myConverted" is a Dictionary&lt;string, long&gt; from the original Dictionary&lt;string, int&gt;</para>
    /// <para>
    /// Only the Dictionary values are affected.</para>
    /// </summary>
    public static dynamic ConvertAsCastable<K, V, W>(this Dictionary<K, V> target, Func<V, W> modifier) where K : notnull where V : notnull where W : notnull
    {
        var result = new Dictionary<K, W>();
        foreach (var t in target)
            result[t.Key] = modifier(t.Value);

        return result;
    }
    #endregion Dictionary

    #region string
    /// <summary>
    /// Returns a local DateTime from a string, if no culture parameter is passed then "InvariantCulture" is used, if no
    /// dateTimeStyle parameter is passed then none is used.
    /// </summary>
    public static DateTime ToDateTime(this string s, CultureInfo? culture = null, DateTimeStyles dateTimeStyle = DateTimeStyles.None) => DateTime.Parse(s, culture ?? CultureInfo.InvariantCulture, dateTimeStyle);
    /// <summary>
    /// Returns an UTC DateTime from a string, if no culture parameter is passed then "InvariantCulture" is used, if no
    /// dateTimeStyle parameter is passed then none is used.
    /// </summary>
    public static DateTime ToDateTimeUTC(this string s, CultureInfo? culture = null, DateTimeStyles dateTimeStyle = DateTimeStyles.None) => DateTime.Parse(s, culture ?? CultureInfo.InvariantCulture, dateTimeStyle).ToUniversalTime();
    #endregion string

    #region DateTime
    /// <summary>
    /// Returns the number of seconds elapsed since 1970-01-01T00:00:00Z.
    /// <para>
    /// WARNING: if your DateTime has an unspecified Kind (UTC, Local, Unspecified),
    /// it will be treated as UTC (so if your DT is e.g. 4 hours back from UTC, it will
    /// look like it is in the past).</para>
    /// </summary>
    public static long ToEpoch(this DateTime dt) => new DateTimeOffset(dt, TimeSpan.Zero).ToUnixTimeSeconds();

    /// <summary>
    /// Returns a customizable string compliant with the ISO8601 standard for date and time storage.
    /// <para>if "isUTC" is false (default) then the local timezone will be used.</para>
    /// </summary>
    public static string ToIso8601(this DateTime dt, bool isYear = true, bool isMonth = true, bool isDay = true, bool isHours = true, bool isMinutes = true, bool isSeconds = true, bool isMilliSeconds = false, bool isUTC = false)
    {
        var size = 0;
        if (isYear) size += 4;
        if (isMonth) size += 3;
        if (isDay) size += 3;
        if (isHours) size += 3;
        if (isMinutes) size += 3;
        if (isSeconds) size += 3;
        if (isMilliSeconds) size += 4;
        if (isUTC) size++; else size += 6;
        Span<char> isoResult = stackalloc char[size];

        if (isYear) dt.Year.ToString().CopyTo(isoResult[0..]);
        if (isMonth)
        {
            isoResult[isoResult.IndexOf('\0')] = '-';
            dt.Month.ToString().PadLeft(2, '0').CopyTo(isoResult[isoResult.IndexOf('\0')..]);
        }
        if (isDay)
        {
            isoResult[isoResult.IndexOf('\0')] = '-';
            dt.Day.ToString().PadLeft(2, '0').CopyTo(isoResult[isoResult.IndexOf('\0')..]);
        }
        if (isHours)
        {
            isoResult[isoResult.IndexOf('\0')] = 'T';
            dt.Hour.ToString().PadLeft(2, '0').CopyTo(isoResult[isoResult.IndexOf('\0')..]);
        }
        if (isMinutes)
        {
            isoResult[isoResult.IndexOf('\0')] = ':';
            dt.Minute.ToString().PadLeft(2, '0').CopyTo(isoResult[isoResult.IndexOf('\0')..]);
        }
        if (isSeconds)
        {
            isoResult[isoResult.IndexOf('\0')] = ':';
            dt.Second.ToString().PadLeft(2, '0').CopyTo(isoResult[isoResult.IndexOf('\0')..]);
        }
        if (isMilliSeconds)
        {
            isoResult[isoResult.IndexOf('\0')] = '.';
            dt.Millisecond.ToString().PadLeft(3, '0').CopyTo(isoResult[isoResult.IndexOf('\0')..]);
        }
        if (isUTC)
            isoResult[isoResult.IndexOf('\0')] = 'Z';
        else
        {
            isoResult[isoResult.IndexOf('\0')] = dt.Hour >= 0 ? '+' : '-';
            var offset = TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow);
            offset.Hours.ToString().PadLeft(2, '0').CopyTo(isoResult[isoResult.IndexOf('\0')..]);
            isoResult[isoResult.IndexOf('\0')] = ':';
            offset.Minutes.ToString().PadLeft(2, '0').CopyTo(isoResult[isoResult.IndexOf('\0')..]);
        }

        return isoResult.ToString();
    }
    #endregion DateTime

    #region long
    /// <summary>
    /// Returns a local timezone DateTime from the epoch.
    /// </summary>
    public static DateTime ToDateTime(this long epoch) => DateTimeOffset.FromUnixTimeSeconds(epoch).LocalDateTime;
    /// <summary>
    /// Returns an UTC timezone DateTime from the epoch
    /// </summary>
    public static DateTime ToDateTimeUTC(this long epoch) => DateTimeOffset.FromUnixTimeSeconds(epoch).UtcDateTime;
    #endregion long

    #region generic
    /// <summary>
    /// Syntactic sugar to check if a value is in a group of the same kind.
    /// </summary>
    public static bool In<T>(this T val, params T[] vals) => vals.Contains(val);
    #endregion generic
}
