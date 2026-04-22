/*
 * jTPCCConsistencyCheck - Periodic validator for TPC-C Clause 3.3
 *     consistency conditions, for long-stability testing.
 *
 * Runs in a dedicated thread with its own read-only JDBC connection.
 * Snapshot mode only: all enabled conditions execute inside one
 * REPEATABLE READ / SERIALIZABLE transaction per cycle. v1 is
 * PostgreSQL-only.
 */

import org.apache.log4j.*;

import java.io.*;
import java.sql.*;
import java.util.*;

public class jTPCCConsistencyCheck implements Runnable
{
    private static Logger log = Logger.getLogger(jTPCCConsistencyCheck.class);

    // All Clause 3.3 condition IDs in scope for v1. IDs not yet
    // implemented are filtered at run time and skipped with a warning.
    private static final int[] ALL_CONDITIONS =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    private final jTPCC             parent;
    private final String            dbUrl;
    private final Properties        dbProps;
    private final int               intervalSec;
    private final List<Integer>     conditions;
    private final int               isolationLevel;
    private final boolean           abortOnFail;
    private final int               runID;
    private final BufferedWriter    csv;

    private Connection              conn;
    private volatile boolean        requestStop = false;
    private int                     checkCounter = 0;
    private final Set<Integer>      warnedUnimplemented = new HashSet<Integer>();

    public jTPCCConsistencyCheck(
            jTPCC parent,
            String dbUrl, Properties dbProps,
            int intervalSec, List<Integer> conditions,
            int isolationLevel, boolean abortOnFail,
            int runID, File resultDataDir) throws IOException
    {
	this.parent = parent;
	this.dbUrl = dbUrl;
	this.dbProps = dbProps;
	this.intervalSec = intervalSec;
	this.conditions = conditions;
	this.isolationLevel = isolationLevel;
	this.abortOnFail = abortOnFail;
	this.runID = runID;

	File f = new File(resultDataDir, "consistency.csv");
	this.csv = new BufferedWriter(new FileWriter(f));
	csv.write("run,checkID,tsMs,conditionID,passed,durationMs," +
		  "firstOffendingKey,detail\n");
	csv.flush();
	log.info("Term-CC, writing consistency results to " + f.getPath());
    }

    public void requestStop()
    {
	requestStop = true;
    }

    public void run()
    {
	try
	{
	    conn = DriverManager.getConnection(dbUrl, dbProps);
	    conn.setAutoCommit(false);
	    conn.setReadOnly(true);
	    conn.setTransactionIsolation(isolationLevel);
	}
	catch (SQLException e)
	{
	    log.error("Term-CC, failed to open checker connection: " +
		      e.getMessage());
	    closeCsv();
	    return;
	}

	log.info("Term-CC, started (interval=" + intervalSec + "s, " +
		 "conditions=" + conditions + ", isolation=" +
		 isolationName(isolationLevel) + ", abortOnFail=" +
		 abortOnFail + ")");

	boolean stoppedDueToFailure = false;
	long nextRunAt = System.currentTimeMillis();   // first cycle ASAP

	while (!requestStop)
	{
	    long now = System.currentTimeMillis();
	    if (now >= nextRunAt)
	    {
		if (runOneCycle())
		{
		    stoppedDueToFailure = true;
		    break;
		}
		nextRunAt = (intervalSec > 0)
			    ? System.currentTimeMillis() + intervalSec * 1000L
			    : Long.MAX_VALUE;
	    }

	    try { Thread.sleep(500); }
	    catch (InterruptedException ie) { break; }
	}

	// Final check at shutdown, unless we're already winding down
	// because of a detected violation.
	if (!stoppedDueToFailure)
	    runOneCycle();

	try { conn.close(); } catch (SQLException e) { /* ignore */ }
	closeCsv();
	log.info("Term-CC, stopped after " + checkCounter + " cycle(s)");
    }

    // Returns true if this cycle triggered an abort-on-fail.
    private boolean runOneCycle()
    {
	checkCounter++;
	int localCheckID = checkCounter;
	long cycleStartMs = System.currentTimeMillis();
	int passed = 0, failed = 0, skipped = 0;

	for (int conditionID : conditions)
	{
	    if (!isImplemented(conditionID))
	    {
		if (warnedUnimplemented.add(conditionID))
		    log.warn("Term-CC, condition " + conditionID +
			     " not yet implemented, skipping");
		skipped++;
		continue;
	    }

	    long tsMs = System.currentTimeMillis();
	    long startNs = System.nanoTime();
	    CheckResult r;
	    try
	    {
		r = runCondition(conditionID);
	    }
	    catch (SQLException e)
	    {
		r = new CheckResult(false, "",
				    "SQLException: " + e.getMessage());
		log.error("Term-CC, condition " + conditionID +
			  " errored: " + e.getMessage());
	    }
	    long durationMs = (System.nanoTime() - startNs) / 1_000_000L;

	    writeCsvRow(localCheckID, tsMs, conditionID, r, durationMs);

	    if (r.passed)
	    {
		passed++;
		log.debug("Term-CC, condition " + conditionID +
			  " passed in " + durationMs + "ms");
	    }
	    else
	    {
		failed++;
		log.error("Term-CC, condition " + conditionID +
			  " FAILED: key=" + r.firstOffendingKey +
			  " detail=" + r.detail);
		if (abortOnFail)
		    break;
	    }
	}

	try { conn.commit(); }
	catch (SQLException e)
	{
	    log.error("Term-CC, commit failed: " + e.getMessage());
	}

	long cycleDurationMs = System.currentTimeMillis() - cycleStartMs;
	log.info("Term-CC, check #" + localCheckID + ": " +
		 passed + " passed, " + failed + " failed, " +
		 skipped + " skipped (" + cycleDurationMs + "ms)");

	if (failed > 0 && abortOnFail)
	{
	    log.error("Term-CC, abortOnFail=true, signalling run to stop");
	    parent.signalConsistencyCheckFailure(
		"consistency check #" + localCheckID + " had " +
		failed + " failure(s)");
	    return true;
	}
	return false;
    }

    private CheckResult runCondition(int conditionID) throws SQLException
    {
	switch (conditionID)
	{
	    case 1: return checkCondition1();
	    default:
		throw new IllegalStateException(
		    "condition " + conditionID + " not implemented " +
		    "(should have been filtered)");
	}
    }

    /*
     * Clause 3.3.2.1: for each warehouse W,
     *     W_YTD = sum(D_YTD) over its districts.
     */
    private CheckResult checkCondition1() throws SQLException
    {
	String sql =
	    "SELECT w.w_id, w.w_ytd, d.sum_d_ytd " +
	    "  FROM bmsql_warehouse w " +
	    "  JOIN (SELECT d_w_id, SUM(d_ytd) AS sum_d_ytd " +
	    "          FROM bmsql_district " +
	    "         GROUP BY d_w_id) d " +
	    "    ON w.w_id = d.d_w_id " +
	    " WHERE w.w_ytd <> d.sum_d_ytd " +
	    " LIMIT 1";
	Statement st = conn.createStatement();
	try
	{
	    ResultSet rs = st.executeQuery(sql);
	    if (rs.next())
	    {
		String key = "w_id=" + rs.getInt(1);
		String detail = "w_ytd=" + rs.getBigDecimal(2) +
				", sum(d_ytd)=" + rs.getBigDecimal(3);
		return new CheckResult(false, key, detail);
	    }
	    return new CheckResult(true, "", "");
	}
	finally
	{
	    try { st.close(); } catch (SQLException e) { /* ignore */ }
	}
    }

    private static boolean isImplemented(int conditionID)
    {
	// Extend in subsequent commits as Tier 1 / 2 / 3 land.
	return conditionID == 1;
    }

    private void writeCsvRow(int checkID, long tsMs, int conditionID,
			     CheckResult r, long durationMs)
    {
	try
	{
	    csv.write(runID + "," + checkID + "," + tsMs + "," +
		      conditionID + "," + (r.passed ? 1 : 0) + "," +
		      durationMs + "," + csvEscape(r.firstOffendingKey) +
		      "," + csvEscape(r.detail) + "\n");
	    csv.flush();
	}
	catch (IOException e)
	{
	    log.error("Term-CC, failed to write consistency.csv: " +
		      e.getMessage());
	}
    }

    private void closeCsv()
    {
	try { csv.close(); } catch (IOException e) { /* ignore */ }
    }

    private static String csvEscape(String s)
    {
	if (s == null) return "";
	if (s.indexOf(',') < 0 && s.indexOf('"') < 0 && s.indexOf('\n') < 0)
	    return s;
	return "\"" + s.replace("\"", "\"\"") + "\"";
    }

    // ---- Static config parse helpers used by jTPCC ----

    public static int parseIntervalSec(String s)
    {
	if (s == null || s.trim().isEmpty()) return 60;
	int v;
	try { v = Integer.parseInt(s.trim()); }
	catch (NumberFormatException e) {
	    throw new IllegalArgumentException(
		"consistencyCheckIntervalSec: not an integer: " + s);
	}
	if (v < 0)
	    throw new IllegalArgumentException(
		"consistencyCheckIntervalSec: must be >= 0");
	return v;
    }

    public static List<Integer> parseConditions(String s)
    {
	List<Integer> out = new ArrayList<Integer>();
	if (s == null || s.trim().isEmpty() ||
	    s.trim().equalsIgnoreCase("all"))
	{
	    for (int id : ALL_CONDITIONS) out.add(id);
	    return out;
	}
	for (String part : s.split(","))
	{
	    part = part.trim();
	    if (part.isEmpty()) continue;
	    int id;
	    try { id = Integer.parseInt(part); }
	    catch (NumberFormatException e) {
		throw new IllegalArgumentException(
		    "consistencyCheckConditions: not an integer: " + part);
	    }
	    if (id < 1 || id > 10)
		throw new IllegalArgumentException(
		    "consistencyCheckConditions: id out of range 1..10: " + id);
	    out.add(id);
	}
	if (out.isEmpty())
	    throw new IllegalArgumentException(
		"consistencyCheckConditions: empty");
	return out;
    }

    public static int parseIsolation(String s)
    {
	if (s == null || s.trim().isEmpty())
	    return Connection.TRANSACTION_REPEATABLE_READ;
	String t = s.trim().toUpperCase();
	if (t.equals("REPEATABLE_READ") || t.equals("REPEATABLE READ"))
	    return Connection.TRANSACTION_REPEATABLE_READ;
	if (t.equals("SERIALIZABLE"))
	    return Connection.TRANSACTION_SERIALIZABLE;
	throw new IllegalArgumentException(
	    "consistencyCheckIsolation: expected REPEATABLE_READ or " +
	    "SERIALIZABLE, got " + s);
    }

    private static String isolationName(int level)
    {
	switch (level)
	{
	    case Connection.TRANSACTION_REPEATABLE_READ: return "REPEATABLE_READ";
	    case Connection.TRANSACTION_SERIALIZABLE:    return "SERIALIZABLE";
	    default: return "level=" + level;
	}
    }

    private static class CheckResult
    {
	final boolean passed;
	final String  firstOffendingKey;
	final String  detail;
	CheckResult(boolean passed, String firstOffendingKey, String detail)
	{
	    this.passed = passed;
	    this.firstOffendingKey = firstOffendingKey;
	    this.detail = detail;
	}
    }
}
