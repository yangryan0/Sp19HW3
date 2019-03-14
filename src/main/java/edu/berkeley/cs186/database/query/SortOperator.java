package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.PageAllocator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.io.Page;

import java.util.*;

public class SortOperator {
    private Database.Transaction transaction;
    private String tableName;
    private Comparator<Record> comparator;
    private Schema operatorSchema;
    private int numBuffers;
    private String sortedTableName = null;

    public SortOperator(Database.Transaction transaction, String tableName,
                        Comparator<Record> comparator) throws DatabaseException, QueryPlanException {
        this.transaction = transaction;
        this.tableName = tableName;
        this.comparator = comparator;
        this.operatorSchema = this.computeSchema();
        this.numBuffers = this.transaction.getNumMemoryPages();
    }

    public Schema computeSchema() throws QueryPlanException {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    public class Run {
        String tempTableName;

        public Run() throws DatabaseException {
            this.tempTableName = SortOperator.this.transaction.createTempTable(
                                     SortOperator.this.operatorSchema);
        }

        public void addRecord(List<DataBox> values) throws DatabaseException {
            SortOperator.this.transaction.addRecord(this.tempTableName, values);
        }

        public void addRecords(List<Record> records) throws DatabaseException {
            for (Record r : records) {
                this.addRecord(r.getValues());
            }
        }

        public Iterator<Record> iterator() throws DatabaseException {
            return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
        }

        public String tableName() {
            return this.tempTableName;
        }
    }

    /**
     * Returns a NEW run that is the sorted version of the input run.
     * Can do an in memory sort over all the records in this run
     * using one of Java's built-in sorting methods.
     * Note: Don't worry about modifying the original run.
     * Returning a new run would bring one extra page in memory beyond the
     * size of the buffer, but it is done this way for ease.
     */
    public Run sortRun(Run run) throws DatabaseException {
        List<Record> records = new ArrayList<>();
        run.iterator().forEachRemaining(records::add);
        Collections.sort(records, this.comparator);
        Run ret = new Run();
        ret.addRecords(records);
        return ret;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result
     * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run next.
     * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
     * where a Pair (r, i) is the Record r with the smallest value you are
     * sorting on currently unmerged from run i.
     */
    public Run mergeSortedRuns(List<Run> runs) throws DatabaseException {
        PriorityQueue<Pair<Record, Integer>> p_queue = new PriorityQueue<>(runs.size(), new RecordPairComparator());
        List<Iterator<Record>> run_records = new ArrayList<>();
        for (Run run: runs) {
            run_records.add(run.iterator());
        }
        Run ret = new Run();
        for (int i = 0; i < run_records.size(); i++) {
            Pair<Record, Integer> n = new Pair<>(run_records.get(i).next(), i);
            p_queue.add(n);
        }
        while (!p_queue.isEmpty()) {
            Pair<Record, Integer> curr = p_queue.poll();
            ret.addRecord(curr.getFirst().getValues());
            try {
                p_queue.add(new Pair<>(run_records.get(curr.getSecond()).next(), curr.getSecond()));
            } catch (Exception e) {
            }
        }
        return ret;

    }

    /**
     * Given a list of N sorted runs, returns a list of
     * sorted runs that is the result of merging (numBuffers - 1)
     * of the input runs at a time.
     */
    public List<Run> mergePass(List<Run> runs) throws DatabaseException {
        List<Run> res = new ArrayList<>();
        for (int i = 0; i < runs.size(); i = i + numBuffers - 1) {
            res.add(mergeSortedRuns(runs.subList(i, i + numBuffers - 1)));
        }
        return res;
    }

    /**
     * Does an external merge sort on the table with name tableName
     * using numBuffers.
     * Returns the name of the table that backs the final run.
     */
    public String sort() throws DatabaseException {
        PageAllocator.PageIterator a = transaction.getPageIterator(tableName);
        a.next();
        Iterator<Record> r = transaction.getBlockIterator(tableName, a);
        List<Run> sorted_runs = new ArrayList<>();
        int records_per_run = transaction.getNumEntriesPerPage(tableName) * numBuffers;
        while (r.hasNext()) {
            Run curr_run = new Run();
            for (int i = 0; i < records_per_run && r.hasNext(); i++) {
                curr_run.addRecord(r.next().getValues());
            }
            sorted_runs.add(sortRun(curr_run));
        }
        return mergeSortedRuns(sorted_runs).tableName();
    }

    public Iterator<Record> iterator() throws DatabaseException {
        if (sortedTableName == null) {
            sortedTableName = sort();
        }
        return this.transaction.getRecordIterator(sortedTableName);
    }

    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());

        }
    }
    public Run createRun() throws DatabaseException {
        return new Run();
    }
}

