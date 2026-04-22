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

    // Cumulative stats, published for the end-of-run summary.
    private volatile int            totalPassed  = 0;
    private volatile int            totalFailed  = 0;
    private volatile int            totalSkipped = 0;
    private volatile String         firstFailureSummary = null;

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
	    log.error("Term-CC, failed to open checker connection: " +
		      e.getMessage());
	    closeCsv();
	    // Silent degradation would turn consistencyCheck=true into a
	    // no-op; treat startup failure the same as a violation.
	    parent.signalConsistencyCheckFailure(
		"checker startup failed: " + e.getMessage());
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
		if (firstFailureSummary == null)
		    firstFailureSummary =
			"check #" + localCheckID + ", condition " +
			conditionID + ", key=" + r.firstOffendingKey +
			", detail=" + r.detail;
		log.error("Term-CC, condition " + conditionID +
			  " FAILED: key=" + r.firstOffendingKey +
			  " detail=" + r.detail);
		if (abortOnFail)
		    break;
	    }
	}

	totalPassed  += passed;
	totalFailed  += failed;
	totalSkipped += skipped;

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
	    case 2: return checkCondition2();
	    case 3: return checkCondition3();
	    case 4: return checkCondition4();
	    case 5: return checkCondition5();
	    case 6: return checkCondition6();
	    case 7: return checkCondition7();
	    case 8: return checkCondition8();
	    case 9: return checkCondition9();
	    case 10: return checkCondition10();
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

    /*
     * Clause 3.3.2.2: for each district,
     *     D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
     *
     * max(NO_O_ID) is allowed to be NULL (all pending new orders
     * delivered); in that case only the ORDER-side equality is
     * enforced. max(O_ID) NULL is treated as a violation since every
     * district has orders from initial load onward.
     */
    private CheckResult checkCondition2() throws SQLException
    {
	String sql =
	    "SELECT d.d_w_id, d.d_id, d.d_next_o_id, " +
	    "       o_agg.max_o_id, no_agg.max_no_o_id " +
	    "  FROM bmsql_district d " +
	    "  LEFT JOIN (SELECT o_w_id, o_d_id, MAX(o_id) AS max_o_id " +
	    "               FROM bmsql_oorder " +
	    "              GROUP BY o_w_id, o_d_id) o_agg " +
	    "    ON d.d_w_id = o_agg.o_w_id AND d.d_id = o_agg.o_d_id " +
	    "  LEFT JOIN (SELECT no_w_id, no_d_id, MAX(no_o_id) AS max_no_o_id " +
	    "               FROM bmsql_new_order " +
	    "              GROUP BY no_w_id, no_d_id) no_agg " +
	    "    ON d.d_w_id = no_agg.no_w_id AND d.d_id = no_agg.no_d_id " +
	    " WHERE o_agg.max_o_id IS NULL " +
	    "    OR (d.d_next_o_id - 1) <> o_agg.max_o_id " +
	    "    OR (no_agg.max_no_o_id IS NOT NULL " +
	    "        AND (d.d_next_o_id - 1) <> no_agg.max_no_o_id) " +
	    " LIMIT 1";
	Statement st = conn.createStatement();
	try
	{
	    ResultSet rs = st.executeQuery(sql);
	    if (rs.next())
	    {
		String key = "d_w_id=" + rs.getInt(1) +
			     ", d_id=" + rs.getInt(2);
		int dNext = rs.getInt(3);
		int maxO = rs.getInt(4);
		boolean maxONull = rs.wasNull();
		int maxNO = rs.getInt(5);
		boolean maxNONull = rs.wasNull();
		String detail = "d_next_o_id=" + dNext +
				", max(o_id)=" + (maxONull ? "NULL" : maxO) +
				", max(no_o_id)=" + (maxNONull ? "NULL" : maxNO);
		return new CheckResult(false, key, detail);
	    }
	    return new CheckResult(true, "", "");
	}
	finally
	{
	    try { st.close(); } catch (SQLException e) { /* ignore */ }
	}
    }

    /*
     * Clause 3.3.2.3: for each district, the set of NO_O_ID values
     * in NEW_ORDER must be contiguous:
     *     max(NO_O_ID) - min(NO_O_ID) + 1 = count(*)
     *
     * Districts with zero pending new orders don't appear in the
     * grouped result and are implicitly OK.
     */
    private CheckResult checkCondition3() throws SQLException
    {
	String sql =
	    "SELECT no_w_id, no_d_id, " +
	    "       MIN(no_o_id) AS min_o, " +
	    "       MAX(no_o_id) AS max_o, " +
	    "       COUNT(*)     AS cnt " +
	    "  FROM bmsql_new_order " +
	    " GROUP BY no_w_id, no_d_id " +
	    "HAVING MAX(no_o_id) - MIN(no_o_id) + 1 <> COUNT(*) " +
	    " LIMIT 1";
	Statement st = conn.createStatement();
	try
	{
	    ResultSet rs = st.executeQuery(sql);
	    if (rs.next())
	    {
		String key = "no_w_id=" + rs.getInt(1) +
			     ", no_d_id=" + rs.getInt(2);
		String detail = "min=" + rs.getInt(3) +
				", max=" + rs.getInt(4) +
				", count=" + rs.getInt(5);
		return new CheckResult(false, key, detail);
	    }
	    return new CheckResult(true, "", "");
	}
	finally
	{
	    try { st.close(); } catch (SQLException e) { /* ignore */ }
	}
    }

    /*
     * Clause 3.3.2.4: for each district,
     *     sum(O_OL_CNT) over ORDER = count(*) over ORDER_LINE
     *
     * Assumes every district has at least one order and one order
     * line (guaranteed after initial load).
     */
    private CheckResult checkCondition4() throws SQLException
    {
	String sql =
	    "SELECT a.w_id, a.d_id, a.sum_ol_cnt, b.count_ol " +
	    "  FROM (SELECT o_w_id AS w_id, o_d_id AS d_id, " +
	    "               SUM(o_ol_cnt) AS sum_ol_cnt " +
	    "          FROM bmsql_oorder " +
	    "         GROUP BY o_w_id, o_d_id) a " +
	    "  JOIN (SELECT ol_w_id AS w_id, ol_d_id AS d_id, " +
	    "               COUNT(*) AS count_ol " +
	    "          FROM bmsql_order_line " +
	    "         GROUP BY ol_w_id, ol_d_id) b " +
	    "    ON a.w_id = b.w_id AND a.d_id = b.d_id " +
	    " WHERE a.sum_ol_cnt <> b.count_ol " +
	    " LIMIT 1";
	Statement st = conn.createStatement();
	try
	{
	    ResultSet rs = st.executeQuery(sql);
	    if (rs.next())
	    {
		String key = "w_id=" + rs.getInt(1) +
			     ", d_id=" + rs.getInt(2);
		String detail = "sum(o_ol_cnt)=" + rs.getLong(3) +
				", count(ol)=" + rs.getLong(4);
		return new CheckResult(false, key, detail);
	    }
	    return new CheckResult(true, "", "");
	}
	finally
	{
	    try { st.close(); } catch (SQLException e) { /* ignore */ }
	}
    }

    /*
     * Clause 3.3.2.5: for each order,
     *     O_CARRIER_ID IS NULL  iff  a matching NEW_ORDER row exists
     *
     * Violation = XOR of the two predicates. A LEFT JOIN from ORDER
     * to NEW_ORDER with a match indicator captures both directions.
     */
    private CheckResult checkCondition5() throws SQLException
    {
	String sql =
	    "SELECT o.o_w_id, o.o_d_id, o.o_id, " +
	    "       CASE WHEN o.o_carrier_id IS NULL THEN 1 ELSE 0 END AS carrier_null, " +
	    "       CASE WHEN no.no_o_id IS NULL THEN 0 ELSE 1 END AS has_new_order " +
	    "  FROM bmsql_oorder o " +
	    "  LEFT JOIN bmsql_new_order no " +
	    "    ON o.o_w_id = no.no_w_id " +
	    "   AND o.o_d_id = no.no_d_id " +
	    "   AND o.o_id   = no.no_o_id " +
	    " WHERE (o.o_carrier_id IS NULL) <> (no.no_o_id IS NOT NULL) " +
	    " LIMIT 1";
	Statement st = conn.createStatement();
	try
	{
	    ResultSet rs = st.executeQuery(sql);
	    if (rs.next())
	    {
		String key = "w_id=" + rs.getInt(1) +
			     ", d_id=" + rs.getInt(2) +
			     ", o_id=" + rs.getInt(3);
		String detail = "o_carrier_id_null=" + rs.getInt(4) +
				", has_new_order=" + rs.getInt(5);
		return new CheckResult(false, key, detail);
	    }
	    return new CheckResult(true, "", "");
	}
	finally
	{
	    try { st.close(); } catch (SQLException e) { /* ignore */ }
	}
    }

    /*
     * Clause 3.3.2.6: for each order,
     *     O_OL_CNT = count(ORDER_LINE rows with the same (W,D,O))
     *
     * COALESCE handles orders with no matching OL rows (would still
     * be a violation unless O_OL_CNT is 0, which the spec doesn't
     * allow). Expensive on large loads — full scan of ORDER and
     * ORDER_LINE.
     */
    private CheckResult checkCondition6() throws SQLException
    {
	String sql =
	    "SELECT o.o_w_id, o.o_d_id, o.o_id, o.o_ol_cnt, " +
	    "       COALESCE(ol_c.cnt, 0) AS ol_cnt " +
	    "  FROM bmsql_oorder o " +
	    "  LEFT JOIN (SELECT ol_w_id, ol_d_id, ol_o_id, " +
	    "                    COUNT(*) AS cnt " +
	    "               FROM bmsql_order_line " +
	    "              GROUP BY ol_w_id, ol_d_id, ol_o_id) ol_c " +
	    "    ON o.o_w_id = ol_c.ol_w_id " +
	    "   AND o.o_d_id = ol_c.ol_d_id " +
	    "   AND o.o_id   = ol_c.ol_o_id " +
	    " WHERE o.o_ol_cnt <> COALESCE(ol_c.cnt, 0) " +
	    " LIMIT 1";
	Statement st = conn.createStatement();
	try
	{
	    ResultSet rs = st.executeQuery(sql);
	    if (rs.next())
	    {
		String key = "w_id=" + rs.getInt(1) +
			     ", d_id=" + rs.getInt(2) +
			     ", o_id=" + rs.getInt(3);
		String detail = "o_ol_cnt=" + rs.getInt(4) +
				", count(ol)=" + rs.getInt(5);
		return new CheckResult(false, key, detail);
	    }
	    return new CheckResult(true, "", "");
	}
	finally
	{
	    try { st.close(); } catch (SQLException e) { /* ignore */ }
	}
    }

    /*
     * Clause 3.3.2.7: for each order line,
     *     OL_DELIVERY_D IS NULL  iff  O_CARRIER_ID IS NULL
     * on the parent order.
     */
    private CheckResult checkCondition7() throws SQLException
    {
	String sql =
	    "SELECT ol.ol_w_id, ol.ol_d_id, ol.ol_o_id, ol.ol_number, " +
	    "       CASE WHEN ol.ol_delivery_d IS NULL THEN 1 ELSE 0 END AS ol_null, " +
	    "       CASE WHEN o.o_carrier_id IS NULL THEN 1 ELSE 0 END AS carrier_null " +
	    "  FROM bmsql_order_line ol " +
	    "  JOIN bmsql_oorder o " +
	    "    ON ol.ol_w_id = o.o_w_id " +
	    "   AND ol.ol_d_id = o.o_d_id " +
	    "   AND ol.ol_o_id = o.o_id " +
	    " WHERE (ol.ol_delivery_d IS NULL) <> (o.o_carrier_id IS NULL) " +
	    " LIMIT 1";
	Statement st = conn.createStatement();
	try
	{
	    ResultSet rs = st.executeQuery(sql);
	    if (rs.next())
	    {
		String key = "w_id=" + rs.getInt(1) +
			     ", d_id=" + rs.getInt(2) +
			     ", o_id=" + rs.getInt(3) +
			     ", ol_number=" + rs.getInt(4);
		String detail = "ol_delivery_d_null=" + rs.getInt(5) +
				", o_carrier_id_null=" + rs.getInt(6);
		return new CheckResult(false, key, detail);
	    }
	    return new CheckResult(true, "", "");
	}
	finally
	{
	    try { st.close(); } catch (SQLException e) { /* ignore */ }
	}
    }

    /*
     * Clause 3.3.2.8: for each warehouse,
     *     W_YTD = sum(H_AMOUNT) over HISTORY rows with H_W_ID = W_ID.
     *
     * Full scan of HISTORY — expensive on long runs.
     */
    private CheckResult checkCondition8() throws SQLException
    {
	String sql =
	    "SELECT w.w_id, w.w_ytd, h.sum_h_amount " +
	    "  FROM bmsql_warehouse w " +
	    "  JOIN (SELECT h_w_id, SUM(h_amount) AS sum_h_amount " +
	    "          FROM bmsql_history " +
	    "         GROUP BY h_w_id) h " +
	    "    ON w.w_id = h.h_w_id " +
	    " WHERE w.w_ytd <> h.sum_h_amount " +
	    " LIMIT 1";
	Statement st = conn.createStatement();
	try
	{
	    ResultSet rs = st.executeQuery(sql);
	    if (rs.next())
	    {
		String key = "w_id=" + rs.getInt(1);
		String detail = "w_ytd=" + rs.getBigDecimal(2) +
				", sum(h_amount)=" + rs.getBigDecimal(3);
		return new CheckResult(false, key, detail);
	    }
	    return new CheckResult(true, "", "");
	}
	finally
	{
	    try { st.close(); } catch (SQLException e) { /* ignore */ }
	}
    }

    /*
     * Clause 3.3.2.9: for each district,
     *     D_YTD = sum(H_AMOUNT) over HISTORY rows with
     *     (H_W_ID, H_D_ID) = (D_W_ID, D_ID).
     */
    private CheckResult checkCondition9() throws SQLException
    {
	String sql =
	    "SELECT d.d_w_id, d.d_id, d.d_ytd, h.sum_h_amount " +
	    "  FROM bmsql_district d " +
	    "  JOIN (SELECT h_w_id, h_d_id, SUM(h_amount) AS sum_h_amount " +
	    "          FROM bmsql_history " +
	    "         GROUP BY h_w_id, h_d_id) h " +
	    "    ON d.d_w_id = h.h_w_id AND d.d_id = h.h_d_id " +
	    " WHERE d.d_ytd <> h.sum_h_amount " +
	    " LIMIT 1";
	Statement st = conn.createStatement();
	try
	{
	    ResultSet rs = st.executeQuery(sql);
	    if (rs.next())
	    {
		String key = "d_w_id=" + rs.getInt(1) +
			     ", d_id=" + rs.getInt(2);
		String detail = "d_ytd=" + rs.getBigDecimal(3) +
				", sum(h_amount)=" + rs.getBigDecimal(4);
		return new CheckResult(false, key, detail);
	    }
	    return new CheckResult(true, "", "");
	}
	finally
	{
	    try { st.close(); } catch (SQLException e) { /* ignore */ }
	}
    }

    /*
     * Clause 3.3.2.10: for each customer,
     *     C_BALANCE = sum(OL_AMOUNT) for delivered order lines of the
     *                 customer   -   sum(H_AMOUNT) for payments of the
     *                                customer.
     *
     * OL_AMOUNT aggregation must join ORDER_LINE to OORDER to recover
     * the O_C_ID — order lines don't carry the customer id directly.
     * HISTORY rows identify the paying customer via H_C_W_ID /
     * H_C_D_ID / H_C_ID. Customers with no delivered lines and no
     * history rows are handled with COALESCE(..., 0).
     *
     * Most expensive check in v1 (two full aggregates joined against
     * CUSTOMER).
     */
    private CheckResult checkCondition10() throws SQLException
    {
	String sql =
	    "SELECT c.c_w_id, c.c_d_id, c.c_id, c.c_balance, " +
	    "       COALESCE(ol_agg.sum_ol, 0) AS sum_delivered, " +
	    "       COALESCE(h_agg.sum_h,  0) AS sum_paid " +
	    "  FROM bmsql_customer c " +
	    "  LEFT JOIN (SELECT o.o_w_id, o.o_d_id, o.o_c_id, " +
	    "                    SUM(ol.ol_amount) AS sum_ol " +
	    "               FROM bmsql_order_line ol " +
	    "               JOIN bmsql_oorder o " +
	    "                 ON ol.ol_w_id = o.o_w_id " +
	    "                AND ol.ol_d_id = o.o_d_id " +
	    "                AND ol.ol_o_id = o.o_id " +
	    "              WHERE ol.ol_delivery_d IS NOT NULL " +
	    "              GROUP BY o.o_w_id, o.o_d_id, o.o_c_id) ol_agg " +
	    "    ON c.c_w_id = ol_agg.o_w_id " +
	    "   AND c.c_d_id = ol_agg.o_d_id " +
	    "   AND c.c_id   = ol_agg.o_c_id " +
	    "  LEFT JOIN (SELECT h_c_w_id, h_c_d_id, h_c_id, " +
	    "                    SUM(h_amount) AS sum_h " +
	    "               FROM bmsql_history " +
	    "              GROUP BY h_c_w_id, h_c_d_id, h_c_id) h_agg " +
	    "    ON c.c_w_id = h_agg.h_c_w_id " +
	    "   AND c.c_d_id = h_agg.h_c_d_id " +
	    "   AND c.c_id   = h_agg.h_c_id " +
	    " WHERE c.c_balance <> " +
	    "       (COALESCE(ol_agg.sum_ol, 0) - COALESCE(h_agg.sum_h, 0)) " +
	    " LIMIT 1";
	Statement st = conn.createStatement();
	try
	{
	    ResultSet rs = st.executeQuery(sql);
	    if (rs.next())
	    {
		String key = "w_id=" + rs.getInt(1) +
			     ", d_id=" + rs.getInt(2) +
			     ", c_id=" + rs.getInt(3);
		String detail = "c_balance=" + rs.getBigDecimal(4) +
				", sum(delivered ol_amount)=" + rs.getBigDecimal(5) +
				", sum(h_amount)=" + rs.getBigDecimal(6);
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
	return conditionID >= 1 && conditionID <= 10;
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
