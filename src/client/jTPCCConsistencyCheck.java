/*
 * jTPCCConsistencyCheck - Periodic validator for TPC-C Clause 3.3
 *     consistency conditions, for long-stability testing.
 *
 * Runs in a dedicated thread with its own read-only JDBC connection.
 * Each condition executes in its own short read-only transaction.
 * Earlier versions wrapped a whole cycle in one snapshot, but with all
 * 12 conditions enabled that single transaction stayed open for many
 * seconds and held back vacuum on the workload tables — splitting per
 * condition trades cross-condition snapshot consistency for shorter
 * per-transaction lifetime, which matters more in long-stability runs.
 *
 * The check queries live in a vendor-aware SQL file:
 *   sql.<db>/consistencyCheck.sql  (override, optional)
 *   sql.common/consistencyCheck.sql (fallback)
 * The file is ;-separated. The Nth statement is condition N. Each
 * query must return zero rows when the condition holds; any returned
 * row is a violation, and the row's columns are logged and written to
 * a per-role CSV. Use LIMIT 1 / FETCH FIRST 1 to keep the payload
 * small.
 *
 * One instance targets one server. The role label ("primary" or
 * "standby") drives the log prefix, the CSV filename, and the failure
 * signal back to jTPCC, so a parallel primary+standby pair can be told
 * apart at a glance: same condition failing on both = workload-logic
 * bug, passing on primary but failing on standby = redo / replication
 * bug.
 */

import org.apache.log4j.*;

import java.io.*;
import java.sql.*;
import java.util.*;

public class jTPCCConsistencyCheck implements Runnable
{
    private static Logger log = Logger.getLogger(jTPCCConsistencyCheck.class);

    public  static final String     ROLE_PRIMARY = "primary";
    public  static final String     ROLE_STANDBY = "standby";

    private final jTPCC             parent;
    private final String            role;       // "primary" or "standby"
    private final String            logPrefix;  // "Term-CC-pri" / "Term-CC-stby"
    private final String            dbUrl;
    private final Properties        dbProps;
    private final int               intervalSec;
    private final List<Integer>     conditions;
    private final int               isolationLevel;
    private final boolean           abortOnFail;
    private final int               runID;
    private final BufferedWriter    csv;
    private final List<String>      sqls;   // index i = condition (i+1)

    private Connection              conn;
    private volatile boolean        requestStop = false;
    private boolean                 connectionDead = false;
    private int                     checkCounter = 0;
    // Last SQL issued by a check, so runOneCycle can log it on violation
    // or SQLException. The checker is single-threaded, so no sync needed.
    private String                  lastSql = null;

    // Cumulative stats, published for the end-of-run summary.
    private volatile int            totalPassed  = 0;
    private volatile int            totalFailed  = 0;
    private volatile int            totalSkipped = 0;
    private volatile String         firstFailureSummary = null;

    public jTPCCConsistencyCheck(
            jTPCC parent, String role,
            String dbUrl, Properties dbProps,
            int intervalSec, List<Integer> conditions,
            int isolationLevel, boolean abortOnFail,
            int runID, File resultDataDir,
            String dbType) throws IOException
    {
	if (!ROLE_PRIMARY.equals(role) && !ROLE_STANDBY.equals(role))
	    throw new IllegalArgumentException(
		"consistency check role must be \"" + ROLE_PRIMARY +
		"\" or \"" + ROLE_STANDBY + "\", got " + role);

	this.parent = parent;
	this.role = role;
	this.logPrefix = ROLE_PRIMARY.equals(role) ? "Term-CC-pri" : "Term-CC-stby";
	this.dbUrl = dbUrl;
	this.dbProps = dbProps;
	this.intervalSec = intervalSec;
	this.isolationLevel = isolationLevel;
	this.abortOnFail = abortOnFail;
	this.runID = runID;

	this.sqls = loadSqlFile(dbType, this.logPrefix);

	// Expand "all" (empty list from parseConditions) to every condition
	// the file supplies, or validate the explicit list against the
	// file's range.
	if (conditions.isEmpty())
	{
	    List<Integer> all = new ArrayList<Integer>();
	    for (int i = 1; i <= sqls.size(); i++) all.add(i);
	    this.conditions = all;
	}
	else
	{
	    for (int id : conditions)
	    {
		if (id < 1 || id > sqls.size())
		    throw new IOException(
			"consistencyCheckConditions: id " + id +
			" out of range 1.." + sqls.size() +
			" (SQL file supplies " + sqls.size() +
			" statement(s))");
	    }
	    this.conditions = conditions;
	}

	// Primary keeps the historical filename consistency.csv; standby
	// gets a suffixed filename so a primary+standby run produces two
	// independent files for after-the-fact comparison.
	String csvName = ROLE_PRIMARY.equals(role)
			 ? "consistency.csv" : "consistency_standby.csv";
	File f = new File(resultDataDir, csvName);
	this.csv = new BufferedWriter(new FileWriter(f));
	csv.write("run,checkID,tsMs,conditionID,passed,durationMs,detail\n");
	csv.flush();
	log.info(logPrefix + ", writing consistency results to " + f.getPath());
    }

    // Resolve sql.<db>/consistencyCheck.sql first, then sql.common/, and
    // split the content on ';' (with \; as literal-semicolon escape) to
    // get one SQL per condition in declaration order.
    private static List<String> loadSqlFile(String dbType, String logPrefix)
	throws IOException
    {
	String name = "consistencyCheck.sql";
	File vendor = new File("sql." + dbType, name);
	File common = new File("sql.common", name);
	File target = vendor.isFile() ? vendor : common;
	if (!target.isFile())
	    throw new IOException("consistency SQL file not found: tried " +
				  vendor.getPath() + " and " +
				  common.getPath());

	StringBuilder buf = new StringBuilder();
	BufferedReader r = new BufferedReader(new FileReader(target));
	try
	{
	    String line;
	    while ((line = r.readLine()) != null)
	    {
		buf.append(line);
		buf.append('\n');
	    }
	}
	finally { try { r.close(); } catch (IOException e) { /* ignore */ } }

	List<String> out = new ArrayList<String>();
	StringBuilder cur = new StringBuilder();
	int len = buf.length();
	for (int i = 0; i < len; i++)
	{
	    char c = buf.charAt(i);
	    if (c == '\\' && i + 1 < len && buf.charAt(i + 1) == ';')
	    {
		cur.append(';');
		i++;
		continue;
	    }
	    if (c == ';')
	    {
		String s = cur.toString().trim();
		if (!s.isEmpty()) out.add(s);
		cur.setLength(0);
		continue;
	    }
	    cur.append(c);
	}
	String tail = cur.toString().trim();
	if (!tail.isEmpty()) out.add(tail);
	if (out.isEmpty())
	    throw new IOException("consistency SQL file " +
				  target.getPath() + " contains no statements");
	log.info(logPrefix + ", loaded " + out.size() +
		 " consistency SQL statement(s) from " + target.getPath());
	return out;
    }

    public void requestStop()
    {
	requestStop = true;
    }

    public String getRole()            { return role; }
    public int getTotalCycles()        { return checkCounter; }
    public int getTotalPassed()        { return totalPassed; }
    public int getTotalFailed()        { return totalFailed; }
    public int getTotalSkipped()       { return totalSkipped; }
    public String getFirstFailureSummary() { return firstFailureSummary; }

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
	    log.error(logPrefix + ", failed to open checker connection: " +
		      e.getMessage());
	    closeCsv();
	    // Silent degradation would turn consistencyCheck=true into a
	    // no-op; treat startup failure the same as a violation.
	    parent.signalConsistencyCheckFailure(role,
		"checker startup failed: " + e.getMessage());
	    return;
	}

	log.info(logPrefix + ", started (url=" + dbUrl +
		 ", interval=" + intervalSec + "s, " +
		 "conditions=" + conditions + ", isolation=" +
		 isolationName(isolationLevel) + ", abortOnFail=" +
		 abortOnFail + ")");

	boolean stoppedDueToFailure = false;
	long nextRunAt = System.currentTimeMillis();   // first cycle ASAP

	while (!requestStop && !connectionDead)
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
	// because of a detected violation or a dead connection we
	// couldn't reopen.
	if (!stoppedDueToFailure && !connectionDead)
	    runOneCycle();

	try { conn.close(); } catch (SQLException e) { /* ignore */ }
	closeCsv();
	log.info(logPrefix + ", stopped after " + checkCounter + " cycle(s)");
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
	    long tsMs = System.currentTimeMillis();
	    long startNs = System.nanoTime();
	    CheckResult r;
	    try
	    {
		r = runCondition(conditionID);
	    }
	    catch (SQLException e)
	    {
		// Hot-standby recovery conflicts are not consistency
		// violations — they're an artifact of running the checker
		// on a replica. PG raises one of two variants:
		//   - "canceling statement ..."   — the query is killed
		//     but the connection survives, so just rollback and
		//     skip this condition.
		//   - "terminating connection ..." — PG killed the whole
		//     backend, so the JDBC conn is dead and every
		//     subsequent query would error out. Reopen before
		//     moving to the next condition; if reopen fails,
		//     stop the checker (don't fail the run).
		if (isRecoveryConflict(e))
		{
		    long durationMs = (System.nanoTime() - startNs) / 1_000_000L;
		    skipped++;
		    log.warn(logPrefix + ", condition " + conditionID +
			     " skipped (standby recovery conflict, " +
			     durationMs + "ms): " + e.getMessage());
		    try { conn.rollback(); }
		    catch (SQLException ee) { /* ignore */ }
		    if (isConnectionTerminated(e) && !reopenConnection())
		    {
			connectionDead = true;
			break;
		    }
		    continue;
		}
		r = new CheckResult(false, "SQLException: " + e.getMessage());
		log.error(logPrefix + ", condition " + conditionID +
			  " errored: " + e.getMessage() +
			  "\n  SQL: " + lastSql);
	    }
	    long durationMs = (System.nanoTime() - startNs) / 1_000_000L;

	    // End the per-condition transaction. On a passed check we
	    // commit; on a violation or non-recovery error the txn is
	    // already poisoned (or we just want to release the snapshot
	    // ASAP), so rollback is fine and avoids commit failures.
	    try
	    {
		if (r.passed) conn.commit();
		else          conn.rollback();
	    }
	    catch (SQLException e)
	    {
		log.error(logPrefix + ", end-of-condition " + conditionID +
			  " " + (r.passed ? "commit" : "rollback") +
			  " failed: " + e.getMessage());
	    }

	    writeCsvRow(localCheckID, tsMs, conditionID, r, durationMs);

	    if (r.passed)
	    {
		passed++;
		log.debug(logPrefix + ", condition " + conditionID +
			  " passed in " + durationMs + "ms");
	    }
	    else
	    {
		failed++;
		if (firstFailureSummary == null)
		    firstFailureSummary =
			"check #" + localCheckID + ", condition " +
			conditionID + ", " + r.detail;
		log.error(logPrefix + ", condition " + conditionID +
			  " FAILED: " + r.detail +
			  "\n  SQL: " + lastSql);
		if (abortOnFail)
		    break;
	    }
	}

	totalPassed += passed;
	totalFailed += failed;
	totalSkipped += skipped;

	long cycleDurationMs = System.currentTimeMillis() - cycleStartMs;
	log.info(logPrefix + ", check #" + localCheckID + ": " +
		 passed + " passed, " + failed + " failed" +
		 (skipped > 0 ? ", " + skipped + " skipped" : "") +
		 " (" + cycleDurationMs + "ms)");

	if (failed > 0 && abortOnFail)
	{
	    log.error(logPrefix + ", abortOnFail=true, signalling run to stop");
	    parent.signalConsistencyCheckFailure(role,
		"consistency check #" + localCheckID + " had " +
		failed + " failure(s)");
	    return true;
	}
	return false;
    }

    // PG raises one of these texts when max_standby_streaming_delay
    // (or _archive_delay) is hit on a hot standby. Match by message
    // rather than SQLState since the state codes (40001 / 57P01) are
    // shared with other transient errors.
    //   - "canceling statement ..."   — query cancelled, conn lives
    //   - "terminating connection ..." — whole backend killed
    private static boolean isRecoveryConflict(SQLException e)
    {
	String m = e.getMessage();
	if (m == null) return false;
	return m.contains("canceling statement due to conflict with recovery")
	    || m.contains("terminating connection due to conflict with recovery");
    }

    private static boolean isConnectionTerminated(SQLException e)
    {
	String m = e.getMessage();
	return m != null &&
	    m.contains("terminating connection due to conflict with recovery");
    }

    // Drop the (likely already-dead) JDBC connection and open a fresh
    // one with the same read-only / isolation settings. Returns true
    // on success.
    private boolean reopenConnection()
    {
	try { conn.close(); } catch (SQLException e) { /* ignore */ }
	try
	{
	    conn = DriverManager.getConnection(dbUrl, dbProps);
	    conn.setAutoCommit(false);
	    conn.setReadOnly(true);
	    conn.setTransactionIsolation(isolationLevel);
	    log.info(logPrefix + ", reopened checker connection after " +
		     "standby recovery conflict");
	    return true;
	}
	catch (SQLException e)
	{
	    log.error(logPrefix + ", failed to reopen checker connection: " +
		      e.getMessage() + " — checker will stop");
	    return false;
	}
    }

    // Execute the SQL for the given condition. If the query returns any
    // row, format all its columns as "col=val, col=val, ..." (using the
    // ResultSet metadata) and report a violation. If it returns no row,
    // the condition holds.
    private CheckResult runCondition(int conditionID) throws SQLException
    {
	String sql = sqls.get(conditionID - 1);
	lastSql = sql;
	Statement st = conn.createStatement();
	try
	{
	    ResultSet rs = st.executeQuery(sql);
	    if (rs.next())
		return new CheckResult(false, formatRow(rs));
	    return new CheckResult(true, "");
	}
	finally
	{
	    try { st.close(); } catch (SQLException e) { /* ignore */ }
	}
    }

    private static String formatRow(ResultSet rs) throws SQLException
    {
	ResultSetMetaData md = rs.getMetaData();
	int n = md.getColumnCount();
	StringBuilder sb = new StringBuilder();
	for (int i = 1; i <= n; i++)
	{
	    if (i > 1) sb.append(", ");
	    sb.append(md.getColumnLabel(i));
	    sb.append('=');
	    Object v = rs.getObject(i);
	    sb.append(v == null ? "NULL" : v.toString());
	}
	return sb.toString();
    }

    private void writeCsvRow(int checkID, long tsMs, int conditionID,
			     CheckResult r, long durationMs)
    {
	try
	{
	    csv.write(runID + "," + checkID + "," + tsMs + "," +
		      conditionID + "," + (r.passed ? 1 : 0) + "," +
		      durationMs + "," + csvEscape(r.detail) + "\n");
	    csv.flush();
	}
	catch (IOException e)
	{
	    log.error(logPrefix + ", failed to write CSV: " + e.getMessage());
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

    // Returns an empty list to mean "all conditions the SQL file supplies"
    // (the checker expands once it has loaded the file). Non-empty input
    // is parsed as an explicit list; upper-bound validation happens at
    // expansion time since the max depends on the SQL file contents.
    public static List<Integer> parseConditions(String s)
    {
	List<Integer> out = new ArrayList<Integer>();
	if (s == null || s.trim().isEmpty() ||
	    s.trim().equalsIgnoreCase("all"))
	{
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
	    if (id < 1)
		throw new IllegalArgumentException(
		    "consistencyCheckConditions: id must be >= 1: " + id);
	    out.add(id);
	}
	if (out.isEmpty())
	    throw new IllegalArgumentException(
		"consistencyCheckConditions: parsed to empty list");
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
	final String  detail;
	CheckResult(boolean passed, String detail)
	{
	    this.passed = passed;
	    this.detail = detail;
	}
    }
}
